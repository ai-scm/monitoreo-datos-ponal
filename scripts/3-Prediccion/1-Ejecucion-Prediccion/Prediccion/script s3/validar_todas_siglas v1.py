#!/usr/bin/env python3

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

import boto3


BUCKET = "pon-prod-ml-iapol"
REGION = "us-east-1"
ROOT_PREFIX = "PrediccionMdP"
HORAS_ESPERADAS = {f"{hora:02d}00" for hora in range(24)}


def semana_actual():
    """Retorna el lunes de la semana actual en formato YYYYMMDD."""
    hoy = date.today()
    lunes = hoy - timedelta(days=hoy.weekday())
    return lunes.strftime("%Y%m%d")


def argumentos():
    parser = argparse.ArgumentParser(
        description=(
            "Valida todas las SIGLAS y unidades desde una semana determinada. "
            "Solo revisa archivos hijos directos de Prediccion/ y comprueba "
            "el formato y las 24 horas de cada día exclusivamente para los "
            "archivos Prediccion. Los archivos PuntosApoyo se ignoran."
        )
    )
    parser.add_argument(
        "--desde",
        default=semana_actual(),
        metavar="YYYYMMDD",
        help=(
            "Semana inicial. Por defecto usa el lunes de la semana actual: "
            f"{semana_actual()}."
        ),
    )
    parser.add_argument(
        "--hasta",
        metavar="YYYYMMDD",
        help=(
            "Semana final. Si se omite, procesa únicamente la semana indicada "
            "en --desde."
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

    for nombre in listar_carpetas(s3, prefix):
        if len(nombre) != 8 or not nombre.isdigit():
            continue
        try:
            fecha_semana = validar_fecha(nombre)
            if (
                fecha_semana >= validar_fecha(fecha_desde)
                and fecha_semana <= validar_fecha(fecha_hasta)
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


def patron_prediccion_unidad(unidad):
    unidad_regex = re.escape(unidad)
    return re.compile(
        rf"^Prediccion_{unidad_regex}_"
        rf"(?P<fecha>\d{{8}})_(?P<hora>\d{{4}})\.geojson$"
    )


def validar_unidad(s3, semana, sigla, unidad):
    """
    Valida exclusivamente archivos hijos directos de Prediccion/.

    Por ejemplo, incluye:
      .../Prediccion/Prediccion_123_20260907_0100.geojson

    Y excluye:
      .../Prediccion/Turno/Prediccion_123_20260907_1.geojson
    """
    prefix = (
        f"{ROOT_PREFIX}/{semana[:4]}/{semana}/"
        f"{sigla}/{unidad}/Prediccion/"
    )
    patron_prediccion = patron_prediccion_unidad(unidad)

    validos_prediccion = 0
    invalidos = []
    cobertura = {"Prediccion": defaultdict(set)}
    paginator = s3.get_paginator("list_objects_v2")

    # Delimiter impide que S3 entregue objetos de Turno/ u otras subcarpetas.
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

            # PuntosApoyo y cualquier otro tipo de archivo quedan fuera
            # del alcance solicitado. No se validan ni se reportan como error.
            if not filename.startswith("Prediccion_"):
                continue

            match_prediccion = patron_prediccion.fullmatch(filename)
            match = match_prediccion

            if match:
                fecha_archivo = match.group("fecha")
                hora_archivo = match.group("hora")
                tipo_archivo = "Prediccion"

                try:
                    validar_fecha(fecha_archivo)
                    fecha_valida = True
                except ValueError:
                    fecha_valida = False

                if fecha_valida and hora_archivo in HORAS_ESPERADAS:
                    cobertura[tipo_archivo][fecha_archivo].add(hora_archivo)
                    validos_prediccion += 1
                    continue

            if not match:
                razon = (
                    "Nombre no cumple: "
                    f"Prediccion_{unidad}_"
                    "YYYYMMDD_HHMM.geojson"
                )
            else:
                razon = (
                    "La fecha no es válida o la hora no corresponde a "
                    "una hora completa entre 0000 y 2300"
                )

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

        # El control de cobertura horaria corresponde solo a Prediccion.
        for tipo_archivo in ("Prediccion",):
            horas_presentes = cobertura[tipo_archivo].get(fecha, set())
            horas_faltantes = sorted(HORAS_ESPERADAS - horas_presentes)

            if horas_faltantes:
                archivos_faltantes = [
                    f"{tipo_archivo}_{unidad}_{fecha}_{hora}.geojson"
                    for hora in horas_faltantes
                ]
                faltantes.append(
                    {
                        "semana": semana,
                        "sigla": sigla,
                        "unidad": unidad,
                        "fecha": fecha,
                        "tipo_archivo": tipo_archivo,
                        "horas_presentes": len(horas_presentes),
                        "cantidad_faltantes": len(horas_faltantes),
                        "horas_faltantes": ";".join(horas_faltantes),
                        "archivos_faltantes": ";".join(archivos_faltantes),
                        "ruta_s3": prefix,
                    }
                )

    return validos_prediccion, invalidos, faltantes


def guardar_csv(ruta, columnas, filas):
    with open(ruta, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=columnas)
        writer.writeheader()
        writer.writerows(filas)


def main():
    args = argumentos()
    validar_fecha(args.desde)
    fecha_hasta = args.hasta or args.desde
    validar_fecha(fecha_hasta)
    if validar_fecha(fecha_hasta) < validar_fecha(args.desde):
        raise ValueError("--hasta no puede ser anterior a --desde")

    marca_tiempo = datetime.now().strftime("%Y%m%d_%H%M%S")
    detalle_csv = f"errores_prediccion_{marca_tiempo}.csv"
    faltantes_csv = f"horas_faltantes_{marca_tiempo}.csv"
    reprocesar_formato_csv = (
        f"unidades_reprocesar_formato_{marca_tiempo}.csv"
    )
    reprocesar_horas_csv = f"unidades_reprocesar_horas_{marca_tiempo}.csv"

    try:
        s3 = conectar_s3()
    except Exception as exc:
        print(f" ❌\n❌ ERROR conectando a S3: {exc}")
        return 1

    print("=" * 88)
    print("  🔍 VALIDACIÓN GENERAL DE PREDICCIONES")
    print("=" * 88)
    print(f"  Desde semana : {args.desde}")
    print(f"  Hasta semana : {fecha_hasta}")
    print(f"  SIGLAS       : {args.sigla.upper() if args.sigla else 'TODAS'}")
    print("  Alcance      : solo hijos directos de Prediccion/; excluye Turno/")
    print("  Proceso 1    : nombres inválidos únicamente de Prediccion")
    print("  Proceso 2    : 24 horas diarias únicamente para Prediccion")
    print("  Ignorado     : todos los archivos PuntosApoyo")
    print("=" * 88)

    semanas = listar_semanas(s3, args.desde, fecha_hasta)
    if not semanas:
        print("❌ No se encontraron semanas para procesar.")
        return 1

    todos_invalidos = []
    todos_faltantes = []
    unidades_reprocesar_formato = defaultdict(int)
    unidades_reprocesar_horas = defaultdict(int)
    total_siglas = 0
    total_unidades = 0
    total_prediccion = 0

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
                    pred, invalidos, faltantes = validar_unidad(
                        s3, semana, sigla, unidad
                    )
                except Exception as exc:
                    print(
                        f"      ⚠️  {unidad}: no se pudo validar: {exc}"
                    )
                    continue

                total_unidades += 1
                total_prediccion += pred
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
                        unidades_reprocesar_formato[clave] += cantidad_invalidos
                        print(
                            f"      ❌ [FORMATO] {unidad}: "
                            f"{cantidad_invalidos} archivo(s) inválido(s)"
                        )
                    if cantidad_faltantes:
                        unidades_reprocesar_horas[clave] += cantidad_faltantes
                        print(
                            f"      ❌ [HORAS]   {unidad}: "
                            f"{cantidad_faltantes} archivo(s) Prediccion faltante(s)"
                        )
                elif args.mostrar_todas:
                    print(
                        f"      ✅ {unidad}: cobertura completa "
                        "(24 horas por día)"
                    )

            estado = (
                "✅" if invalidos_sigla == 0 and faltantes_sigla == 0 else "❌"
            )
            print(
                f"      {estado} Resumen {sigla} | "
                f"FORMATO: {invalidos_sigla} | "
                f"HORAS PREDICCION: {faltantes_sigla}"
            )

    cola_reproceso_formato = [
        {
            "semana": semana,
            "sigla": sigla,
            "unidad": unidad,
            "archivos_invalidos": cantidad,
            "motivo": "FORMATO_INVALIDO",
            "estado": "PENDIENTE_REPROCESO_FORMATO",
        }
        for (semana, sigla, unidad), cantidad in sorted(
            unidades_reprocesar_formato.items()
        )
    ]
    cola_reproceso_horas = [
        {
            "semana": semana,
            "sigla": sigla,
            "unidad": unidad,
            "archivos_prediccion_faltantes": cantidad,
            "motivo": "HORAS_PREDICCION_FALTANTES",
            "estado": "PENDIENTE_REPROCESO_HORAS",
        }
        for (semana, sigla, unidad), cantidad in sorted(
            unidades_reprocesar_horas.items()
        )
    ]

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
    columnas_reproceso_formato = [
        "semana",
        "sigla",
        "unidad",
        "archivos_invalidos",
        "motivo",
        "estado",
    ]
    columnas_reproceso_horas = [
        "semana",
        "sigla",
        "unidad",
        "archivos_prediccion_faltantes",
        "motivo",
        "estado",
    ]
    columnas_faltantes = [
        "semana",
        "sigla",
        "unidad",
        "fecha",
        "tipo_archivo",
        "horas_presentes",
        "cantidad_faltantes",
        "horas_faltantes",
        "archivos_faltantes",
        "ruta_s3",
    ]

    guardar_csv(detalle_csv, columnas_detalle, todos_invalidos)
    guardar_csv(faltantes_csv, columnas_faltantes, todos_faltantes)
    guardar_csv(
        reprocesar_formato_csv,
        columnas_reproceso_formato,
        cola_reproceso_formato,
    )
    guardar_csv(
        reprocesar_horas_csv,
        columnas_reproceso_horas,
        cola_reproceso_horas,
    )

    print("\n" + "=" * 88)
    print("  📊 RESUMEN FINAL")
    print("=" * 88)
    print(f"  Semanas procesadas             : {len(semanas)}")
    print(f"  SIGLAS procesadas              : {total_siglas}")
    print(f"  Unidades procesadas            : {total_unidades}")
    print(f"  Archivos Prediccion válidos    : {total_prediccion}")
    print("  --- PROCESO 1: FORMATO ---")
    print(f"  Archivos inválidos             : {len(todos_invalidos)}")
    print(
        "  Unidades por formato           : "
        f"{len(cola_reproceso_formato)}"
    )
    print(f"  Detalle                        : {detalle_csv}")
    print(f"  Cola de reproceso              : {reprocesar_formato_csv}")
    print("  --- PROCESO 2: COBERTURA HORARIA PREDICCION ---")
    print(
        "  Archivos Prediccion faltantes  : "
        f"{sum(f['cantidad_faltantes'] for f in todos_faltantes)}"
    )
    print(
        "  Unidades por horas             : "
        f"{len(cola_reproceso_horas)}"
    )
    print(f"  Detalle                        : {faltantes_csv}")
    print(f"  Cola de reproceso              : {reprocesar_horas_csv}")
    print("=" * 88)

    if cola_reproceso_formato or cola_reproceso_horas:
        print(
            "\n⚠️  El script identificó las unidades pendientes, pero no ejecutó "
            "reprocesos."
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
