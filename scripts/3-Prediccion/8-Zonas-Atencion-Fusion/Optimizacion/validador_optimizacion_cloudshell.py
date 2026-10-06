#!/usr/bin/env python3

import argparse
import csv
import re
import sys
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import boto3


BUCKET = "pon-prod-ml-iapol"
REGION = "us-east-1"
ROOT_PREFIX = "PrediccionMdP"
ARCHIVOS_ESPERADOS_OPTIMIZACION = {
    f"{numero}_F_{sufijo}.geojson"
    for numero in ["1", "2", "3"]
    for sufijo in ["B01", "CP01"]
}
UTC_MENOS_5 = timezone(timedelta(hours=-5))


def semana_actual():
    """Retorna el lunes actual en UTC-5, en formato YYYYMMDD."""
    hoy = datetime.now(UTC_MENOS_5).date()
    lunes = hoy - timedelta(days=hoy.weekday())
    return lunes.strftime("%Y%m%d")


def semana_siguiente():
    """Retorna el lunes de la semana siguiente en UTC-5, en formato YYYYMMDD."""
    hoy = datetime.now(UTC_MENOS_5).date()
    lunes = hoy - timedelta(days=hoy.weekday())
    lunes_siguiente = lunes + timedelta(days=7)
    return lunes_siguiente.strftime("%Y%m%d")


def argumentos():
    parser = argparse.ArgumentParser(
        description=(
            "Valida todas las SIGLAS y unidades desde una semana determinada. "
            "Solo revisa archivos hijos directos de Optimizacion/ y comprueba "
            "el formato y que cada día tenga los 6 archivos requeridos: "
            "1_F_B01, 1_F_CP01, 2_F_B01, 2_F_CP01, 3_F_B01, 3_F_CP01."
        )
    )
    parser.add_argument(
        "--desde",
        default=semana_siguiente(),
        metavar="YYYYMMDD",
        help=(
            "Semana inicial. Por defecto la semana siguiente: "
            f"{semana_siguiente()}."
        ),
    )
    parser.add_argument(
        "--hasta",
        default=semana_siguiente(),
        metavar="YYYYMMDD",
        help=(
            "Semana final. Por defecto la semana siguiente: "
            f"{semana_siguiente()}."
        ),
    )
    parser.add_argument(
        "--sigla",
        help="Opcional: procesa únicamente esta SIGLA; por defecto procesa todas.",
    )
    parser.add_argument(
        "--mostrar-todas",
        action="store_true",
        help="Muestra también las unidades sin errores.",
    )
    return parser.parse_args()


def validar_fecha(fecha_str):
    try:
        return datetime.strptime(fecha_str, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"Fecha inválida: {fecha_str}. Use YYYYMMDD.") from exc


def conectar_s3():
    print("🔗 Conectando a AWS S3...", end="", flush=True)
    cliente = boto3.client("s3", region_name=REGION)
    cliente.head_bucket(Bucket=BUCKET)
    print(" ✅")
    return cliente


def listar_carpetas(s3, prefix):
    """Lista solamente las carpetas hijas directas de un prefijo."""
    carpetas = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
        Delimiter="/",
    ):
        for item in page.get("CommonPrefixes", []):
            nombre = item["Prefix"].rstrip("/").split("/")[-1]
            if nombre:
                carpetas.append(nombre)

    return sorted(set(carpetas))


def listar_semanas(s3, fecha_desde, fecha_hasta):
    """Busca las semanas existentes dentro del intervalo solicitado."""
    anio = fecha_desde[:4]
    prefix = f"{ROOT_PREFIX}/{anio}/"
    semanas = []
    fecha_desde_obj = validar_fecha(fecha_desde)
    fecha_hasta_obj = validar_fecha(fecha_hasta) if fecha_hasta else None

    for nombre in listar_carpetas(s3, prefix):
        if len(nombre) != 8 or not nombre.isdigit():
            continue
        try:
            fecha_semana = validar_fecha(nombre)
            if (
                fecha_semana >= fecha_desde_obj
                and (
                    fecha_hasta_obj is None
                    or fecha_semana <= fecha_hasta_obj
                )
            ):
                semanas.append(nombre)
        except ValueError:
            continue

    return sorted(semanas)


def listar_siglas(s3, semana, sigla_filtro=None):
    prefix = f"{ROOT_PREFIX}/{semana[:4]}/{semana}/"
    siglas = listar_carpetas(s3, prefix)

    if sigla_filtro:
        sigla_filtro = sigla_filtro.upper()
        return [sigla_filtro] if sigla_filtro in siglas else []

    return siglas


def listar_unidades(s3, semana, sigla):
    prefix = f"{ROOT_PREFIX}/{semana[:4]}/{semana}/{sigla}/"
    return listar_carpetas(s3, prefix)


def patron_optimizacion_unidad(unidad):
    """Genera patrón regex para archivos de optimización."""
    unidad_regex = re.escape(unidad)
    return re.compile(
        rf"^Optimizacion_{unidad_regex}_"
        rf"(?P<fecha>\d{{8}})_"
        rf"(?P<numero>[123])_F_(?P<sufijo>B01|CP01)"
        rf"\.geojson$"
    )


def guardar_csv_en_zip(zip_file, nombre_archivo, columnas, filas):
    """Guarda CSV directamente en archivo ZIP."""
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=columnas)
    writer.writeheader()
    writer.writerows(filas)
    zip_file.writestr(nombre_archivo, csv_buffer.getvalue())


def validar_unidad(s3, semana, sigla, unidad):
    """
    Valida exclusivamente archivos hijos directos de Optimizacion/.

    Por ejemplo, incluye:
      .../Optimizacion/Optimizacion_123_20261003_1_F_B01.geojson

    Y excluye subcarpetas dentro de Optimizacion/.
    """
    prefix = (
        f"{ROOT_PREFIX}/{semana[:4]}/{semana}/"
        f"{sigla}/{unidad}/Optimizacion/"
    )
    patron_optimizacion = patron_optimizacion_unidad(unidad)

    validos_optimizacion = 0
    invalidos = []
    cobertura = defaultdict(lambda: defaultdict(set))  # {fecha: {tipo_archivo: set(numeros_sufijos)}}
    paginator = s3.get_paginator("list_objects_v2")

    # Delimiter impide que S3 entregue objetos de subcarpetas.
    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
        Delimiter="/",
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            ruta_relativa = key[len(prefix) :]

            # Defensa adicional para garantizar que sea un hijo directo.
            if not ruta_relativa or "/" in ruta_relativa:
                continue

            filename = ruta_relativa

            # Solo procesamos archivos que comiencen con Optimizacion_
            if not filename.startswith("Optimizacion_"):
                continue

            match_optimizacion = patron_optimizacion.fullmatch(filename)
            match = match_optimizacion

            if match:
                fecha_archivo = match.group("fecha")
                numero_archivo = match.group("numero")
                sufijo_archivo = match.group("sufijo")
                tipo_archivo = "Optimizacion"

                try:
                    validar_fecha(fecha_archivo)
                    fecha_valida = True
                except ValueError:
                    fecha_valida = False

                if fecha_valida:
                    clave_archivo = f"{numero_archivo}_F_{sufijo_archivo}"
                    cobertura[fecha_archivo][tipo_archivo].add(clave_archivo)
                    validos_optimizacion += 1
                    continue

            if not match:
                razon = (
                    "Nombre no cumple: "
                    f"Optimizacion_{unidad}_"
                    "YYYYMMDD_[1|2|3]_F_[B01|CP01].geojson"
                )
            else:
                razon = "La fecha no es válida"

            invalidos.append(
                {
                    "semana": semana,
                    "sigla": sigla,
                    "unidad": unidad,
                    "archivo": filename,
                    "ruta_s3": key,
                    "tamaño_kb": round(obj["Size"] / 1024, 1),
                    "fecha_modificacion": obj["LastModified"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "tipo": "INVALIDO",
                    "razon": razon,
                }
            )

    # La carpeta de semana representa siete días: semana ... semana + 6.
    inicio_semana = validar_fecha(semana).date()
    faltantes = []

    for desplazamiento in range(7):
        fecha = (inicio_semana + timedelta(days=desplazamiento)).strftime(
            "%Y%m%d"
        )

        # El control de archivos requeridos corresponde solo a Optimizacion.
        tipo_archivo = "Optimizacion"
        archivos_presentes = cobertura[fecha][tipo_archivo]
        archivos_faltantes = sorted(
            ARCHIVOS_ESPERADOS_OPTIMIZACION - archivos_presentes
        )

        if archivos_faltantes:
            archivos_faltantes_full = [
                f"Optimizacion_{unidad}_{fecha}_{archivo}"
                for archivo in archivos_faltantes
            ]
            faltantes.append(
                {
                    "semana": semana,
                    "sigla": sigla,
                    "unidad": unidad,
                    "fecha": fecha,
                    "tipo_archivo": tipo_archivo,
                    "archivos_presentes": len(archivos_presentes),
                    "cantidad_faltantes": len(archivos_faltantes),
                    "archivos_faltantes": ";".join(archivos_faltantes),
                    "archivos_faltantes_full": ";".join(archivos_faltantes_full),
                    "ruta_s3": prefix,
                }
            )

    return validos_optimizacion, invalidos, faltantes


def etiqueta_rango(fecha_desde, fecha_hasta):
    """Construye 2026_0831-0909 o incluye ambos años si son distintos."""
    inicio = validar_fecha(fecha_desde)
    fin = validar_fecha(fecha_hasta)

    if inicio.year == fin.year:
        return f"{inicio:%Y}_{inicio:%m%d}-{fin:%m%d}"

    return f"{inicio:%Y_%m%d}-{fin:%Y_%m%d}"


def crear_zip_resultados(sigla):
    """Crea archivo ZIP para resultados usando fecha y hora fija UTC-5."""
    ahora_utc_menos_5 = datetime.now(UTC_MENOS_5)
    sufijo_sigla = f"_{sigla.upper()}" if sigla else ""
    nombre = (
        f"ResultadoOptimizacion{sufijo_sigla}_"
        f"{ahora_utc_menos_5:%Y%m%d_%H%M}.zip"
    )
    ruta_zip = Path.cwd() / nombre
    return ruta_zip, zipfile.ZipFile(ruta_zip, "w", zipfile.ZIP_DEFLATED)


def main():
    args = argumentos()
    validar_fecha(args.desde)
    fecha_hasta = args.hasta
    if fecha_hasta:
        validar_fecha(fecha_hasta)
        if validar_fecha(fecha_hasta) < validar_fecha(args.desde):
            raise ValueError("--hasta no puede ser anterior a --desde")

    try:
        s3 = conectar_s3()
    except Exception as exc:
        print(f" ❌\n❌ ERROR conectando a S3: {exc}")
        return 1

    print("=" * 88)
    print("  🔍 VALIDACIÓN GENERAL DE OPTIMIZACIÓN")
    print("=" * 88)
    print(f"  Desde semana : {args.desde}")
    print(
        "  Hasta semana : "
        f"{fecha_hasta if fecha_hasta else 'ÚLTIMA DISPONIBLE EN S3'}"
    )
    print(f"  SIGLAS       : {args.sigla.upper() if args.sigla else 'TODAS'}")
    print("  Alcance      : solo hijos directos de Optimizacion/")
    print("  Proceso 1    : nombres inválidos de Optimizacion")
    print("  Proceso 2    : 6 archivos requeridos por día")
    print("                 (1_F_B01, 1_F_CP01, 2_F_B01, 2_F_CP01, 3_F_B01, 3_F_CP01)")
    print("=" * 88)

    semanas = listar_semanas(s3, args.desde, fecha_hasta)
    if not semanas:
        print("❌ No se encontraron semanas para procesar.")
        return 1

    ruta_zip, zip_file = crear_zip_resultados(args.sigla)
    fecha_final_efectiva = fecha_hasta or semanas[-1]
    rango = etiqueta_rango(args.desde, fecha_final_efectiva)
    nombre_detalle = f"errores_optimizacion_rango_{rango}.csv"
    nombre_faltantes = f"archivos_faltantes_optimizacion_rango_{rango}.csv"

    print(f"  Resultados    : {ruta_zip}")
    print(f"  Última semana : {semanas[-1]}")

    todos_invalidos = []
    todos_faltantes = []
    unidades_con_error_formato = set()
    unidades_con_archivos_faltantes = set()
    total_siglas = 0
    total_unidades = 0
    total_optimizacion = 0

    for semana in semanas:
        siglas = listar_siglas(s3, semana, args.sigla)
        print(f"\n📅 Semana {semana}: {len(siglas)} SIGLAS")

        for indice_sigla, sigla in enumerate(siglas, 1):
            unidades = listar_unidades(s3, semana, sigla)
            total_siglas += 1
            invalidos_sigla = 0
            faltantes_sigla = 0

            print(
                f"  [{indice_sigla:2d}/{len(siglas)}] {sigla}: "
                f"{len(unidades)} unidades"
            )

            for unidad in unidades:
                try:
                    opt, invalidos, faltantes = validar_unidad(
                        s3, semana, sigla, unidad
                    )
                except Exception as exc:
                    print(
                        f"      ⚠️  {unidad}: no se pudo validar: {exc}"
                    )
                    continue

                total_unidades += 1
                total_optimizacion += opt
                cantidad_invalidos = len(invalidos)
                cantidad_faltantes = sum(
                    fila["cantidad_faltantes"] for fila in faltantes
                )

                if cantidad_invalidos or cantidad_faltantes:
                    invalidos_sigla += cantidad_invalidos
                    faltantes_sigla += cantidad_faltantes
                    todos_invalidos.extend(invalidos)
                    todos_faltantes.extend(faltantes)

                    clave = (semana, sigla, unidad)
                    if cantidad_invalidos:
                        unidades_con_error_formato.add(clave)
                        print(
                            f"      ❌ [FORMATO] {unidad}: "
                            f"{cantidad_invalidos} archivo(s) inválido(s)"
                        )
                    if cantidad_faltantes:
                        unidades_con_archivos_faltantes.add(clave)
                        print(
                            f"      ❌ [ARCHIVOS] {unidad}: "
                            f"{cantidad_faltantes} archivo(s) faltante(s)"
                        )
                elif args.mostrar_todas:
                    print(
                        f"      ✅ {unidad}: completo "
                        "(6 archivos por día)"
                    )

            estado = (
                "✅" if invalidos_sigla == 0 and faltantes_sigla == 0 else "❌"
            )
            print(
                f"      {estado} Resumen {sigla} | "
                f"FORMATO: {invalidos_sigla} | "
                f"ARCHIVOS FALTANTES: {faltantes_sigla}"
            )

    # Cerrar ZIP
    columnas_detalle = [
        "semana",
        "sigla",
        "unidad",
        "archivo",
        "ruta_s3",
        "tamaño_kb",
        "fecha_modificacion",
        "tipo",
        "razon",
    ]
    columnas_faltantes = [
        "semana",
        "sigla",
        "unidad",
        "fecha",
        "tipo_archivo",
        "archivos_presentes",
        "cantidad_faltantes",
        "archivos_faltantes",
        "archivos_faltantes_full",
        "ruta_s3",
    ]

    guardar_csv_en_zip(zip_file, nombre_detalle, columnas_detalle, todos_invalidos)
    guardar_csv_en_zip(zip_file, nombre_faltantes, columnas_faltantes, todos_faltantes)

    zip_file.close()

    print("\n" + "=" * 88)
    print("  📊 RESUMEN FINAL")
    print("=" * 88)
    print(f"  Semanas procesadas             : {len(semanas)}")
    print(f"  SIGLAS procesadas              : {total_siglas}")
    print(f"  Unidades procesadas            : {total_unidades}")
    print(f"  Archivos Optimizacion válidos  : {total_optimizacion}")
    print("  --- PROCESO 1: FORMATO ---")
    print(f"  Archivos inválidos             : {len(todos_invalidos)}")
    print(
        "  Unidades por formato           : "
        f"{len(unidades_con_error_formato)}"
    )
    print(f"  Detalle (en ZIP)               : {nombre_detalle}")
    print("  --- PROCESO 2: ARCHIVOS REQUERIDOS ---")
    print(
        "  Archivos faltantes (total días): "
        f"{sum(f['cantidad_faltantes'] for f in todos_faltantes)}"
    )
    print(
        "  Unidades con faltantes         : "
        f"{len(unidades_con_archivos_faltantes)}"
    )
    print(f"  Detalle (en ZIP)               : {nombre_faltantes}")
    print(f"  Archivo ZIP                    : {ruta_zip}")
    print("=" * 88)

    if unidades_con_error_formato or unidades_con_archivos_faltantes:
        print(
            "\n⚠️  Se encontraron observaciones. Consulte los dos CSV en el ZIP."
        )
    else:
        print("\n✅ No hay unidades para reprocesar.")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n⏹️  Interrumpido.")
        sys.exit(130)
    except Exception as exc:
        print(f"\n❌ ERROR: {exc}")
        sys.exit(1)
