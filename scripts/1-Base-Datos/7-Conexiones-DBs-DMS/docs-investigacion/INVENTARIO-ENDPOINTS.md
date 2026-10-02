# Inventario de endpoints DMS (Oracle) — región us-east-1

Inventario de los endpoints DMS con engine Oracle, para saber qué host:puerto
tiene cada base y poder probar conectividad (ver `PRUEBA-VPC-ENVIRONMENT.md`).

Generado con:

```bash
aws dms describe-endpoints \
  --query "Endpoints[?EngineName=='oracle'].{Id:EndpointIdentifier,Host:ServerName,Port:Port,DB:DatabaseName}" \
  --output table
```

> Para regenerarlo cuando cambien los endpoints, vuelve a correr ese comando.
> Fecha de esta captura: 2026-09-30.

## Resumen

- **Endpoints Oracle con host configurado (probables de pinguear): 66.**
- La mayoría de las bases `secad` usan **hostname DNS** `<SIGLA>.secad.prod.mdp.local:1521`.
- Algunas usan **IP directa** (marcadas abajo).
- Los `pon-prod-raw-secad-*-source-endpoint` están **sin ServerName/Port (None)**:
  son endpoints creados pero sin configurar; no hay host que pinguear (ver sección
  aparte).

## secad — por hostname DNS (puerto 1521)

| Sigla / Base | Host | Endpoint DMS |
|---|---|---|
| MEBUC | `MEBUC.secad.prod.mdp.local` | `bucaramanga-secad-source`, `mebuc-secad-prod-source` |
| DEAMA | `deama.secad.prod.mdp.local` | `deama-secad-prod-source` |
| DEANT | `DEANT.secad.prod.mdp.local` | `deant-secad-prod-source` |
| DEARA | `DEARA.secad.prod.mdp.local` | `deara-secad-prod-source` |
| DEATA | `DEATA.secad.prod.mdp.local` | `deata-secad-prod-source` |
| DEBOY | `DEBOY.secad.prod.mdp.local` | `deboy-secad-prod-source` |
| DECAQ | `DECAQ.secad.prod.mdp.local` | `decaq-secad-prod-source` |
| DECAS | `DECAS.secad.prod.mdp.local` | `decas-secad-prod-source` |
| DECAU | `DECAU.secad.prod.mdp.local` | `decau-secad-prod-source` |
| DECES | `DECES.secad.prod.mdp.local` | `deces-secad-prod-source` |
| DECHO | `DECHO.secad.prod.mdp.local` | `decho-secad-prod-source` |
| DECOR | `DECOR.secad.prod.mdp.local` | `decor-secad-prod-source` |
| DECUN | `DECUN.secad.prod.mdp.local` | `decun-secad-prod-source` |
| DEGUA | `DEGUA.secad.prod.mdp.local` | `degua-secad-prod-source` |
| DEGUN | `DEGUN.secad.prod.mdp.local` | `degun-secad-prod-source` |
| DEGUV | `DEGUV.secad.prod.mdp.local` | `deguv-secad-prod-source` |
| DEMAG | `DEMAG.secad.prod.mdp.local` | `demag-secad-prod-source` |
| DEMAM | `DEMAM.secad.prod.mdp.local` | `demam-secad-prod-source` |
| DEMET | `DEMET.secad.prod.mdp.local` | `demet-secad-prod-source` |
| DENAR | `DENAR.secad.prod.mdp.local` | `denar-secad-prod-source` |
| DENOR | `DENOR.secad.prod.mdp.local` | `denor-secad-prod-source` |
| DEPUY | `DEPUY.secad.prod.mdp.local` | `depuy-secad-prod-source` |
| DEQUI | `DEQUI.secad.prod.mdp.local` | `dequi-secad-prod-source` |
| DERIS | `DERIS.secad.prod.mdp.local` | `deris-secad-prod-source` |
| DESAN | `DESAN.secad.prod.mdp.local` | `desan-secad-prod-source` |
| DETOL | `DETOL.secad.prod.mdp.local` | `detol-secad-prod-source` |
| DEUIL | `DEUIL.secad.prod.mdp.local` | `deuil-secad-prod-source` |
| DEURA | `DEURA.secad.prod.mdp.local` | `deura-secad-prod-source` |
| DEVAL | `DEVAL.secad.prod.mdp.local` | `deval-secad-prod-source` |
| DEVAU | `DEVAU.secad.prod.mdp.local` | `devau-secad-prod-source` |
| DEVIC | `DEVIC.secad.prod.mdp.local` | `devic-secad-prod-source` |
| DUITAMA | `duitama.secad.prod.mdp.local` | `duitama-secad-prod-source` |
| MEBAR | `MEBAR.secad.prod.mdp.local` | `mebar-secad-prod-source` |
| MECAL | `MECAL.secad.prod.mdp.local` | `mecal-secad-prod-source` |
| MECAR | `MECAR.secad.prod.mdp.local` | `mecar-secad-prod-source` |
| MECUC | `MECUC.secad.prod.mdp.local` | `mecuc-secad-prod-source`, `mecuc-secad-prod-source1` |
| MEMAZ | `MEMAZ.secad.prod.mdp.local` | `memaz-secad-prod-source` |
| MENEV | `MENEV.secad.prod.mdp.local` | `menev-secad-prod-source` |
| MEPER | `MEPER.secad.prod.mdp.local` | `meper-secad-prod-source` |
| MEPOY | `MEPOY.secad.prod.mdp.local` | `mepoy-secad-prod-source` |
| MESAN | `MESAN.secad.prod.mdp.local` | `mesan-secad-prod-source` |
| METIB | `METIB.secad.prod.mdp.local` | `metib-secad-prod-source` |
| METUN | `metun.secad.prod.mdp.local` | `metun-secad-prod-source` |
| MEVIL | `MEVIL.secad.prod.mdp.local` | `mevil-secad-prod-source` |
| SOGAMOSO | `sogamoso.secad.prod.mdp.local` | `sogamoso-secad-prod-source` |
| PRUEBAS | `pruebas.secad.prod.mdp.local` | `pruebas-secad-prod-source` |

## secad — por IP directa (puerto 1521)

| Sigla / Base | Host (IP) | Endpoint DMS |
|---|---|---|
| DEBOL | `172.26.227.4` | `debol-secad-prod-source` |
| DECAL | `172.26.42.253` | `decal-secad-prod-source` |
| DESAP | `172.27.25.10` | `desap-secad-prod-source` |
| DESUC | `172.26.226.2` | `desuc-secad-prod-source` |
| MEMOT | `172.26.105.220` | `memot-secad-prod-source` |
| MEPAS | `172.22.66.170` | `mepas-secad-prod-source` |

## secad — nacional (puerto 1538)

| Base | Host | Endpoint DMS |
|---|---|---|
| secad (nacional) | `secad.prod.mdp.local` | `nacional-secad-prod-source`, `secad-prod-source` |

## Bases del pipeline principal (no secad)

| Base | Host | Puerto | Endpoint DMS |
|---|---|---|---|
| DISEC | `172.28.18.221` | 1538 | `disec-endpoint-source`, `pon-prod-disec-source`, `pon-test-disec`, `pon-test-disec-dms`, `pon-test-diseendpoint` |
| SIVICC | `172.28.18.185` | 1538 | `sivicc-antecedentes-source` |
| SIVICC | `sivicc2.prod.mdp.local` | 1538 | `sivicc-prod-source` |
| SIVICC (test) | `10.41.3.152` | 11521 | `pon-test-dms-sivvic` |
| SIEDCO | `siedco.prod.mdp.local` | 1538 | `siedco-prod-source`, `siedco-copy-prod-source` |
| GEOPOL | `172.28.18.194` | 1538 | `pon-test-geopol-dms` |
| TELEPOL | `172.28.18.194` | 1538 | `pon-test-geopol` |
| TELEPOL | `172.28.18.238` | 1538 | `pon-test-telepol-dms` |
| RNMC / cnp | `cnp.prod.mdp.local` | 1538 | `rnmc-prod-source` |
| RNMC (test) | `172.28.18.244` | 1538 | `pon-prod-rnmc-test` |

> Nota: hay endpoints marcados `test`/`copy` que pueden ser duplicados o de prueba;
> conviene confirmar cuáles son los productivos con el equipo de datos.

## Endpoints sin configurar (ServerName/Port = None)

Los siguientes existen en DMS pero **no tienen host ni puerto** (aparecen como
`None`). No se pueden pinguear porque no tienen destino configurado. Son los
`pon-prod-raw-secad-<SIGLA>-source-endpoint` (uno por casi cada sigla: deama,
deant, deara, deata, debol, deboy, decal, decaq, decas, decau, deces, decho,
decor, decun, degua, degun, deguv, demag, demam, demet, denar, denor, depuy,
dequi, deris, desan, desap, desuc, detol, deuil, deura, deval, devau, devic,
mebar, mebuc, mecal, mecar, mecuc, memaz, memot, menev, mepas, meper, mepoy,
mesan, metib, metun, meval, mevil).

> Estos parecen endpoints "placeholder" del pipeline raw que aún no fueron
> configurados con su ServerName. Vale la pena revisar con el equipo si deben
> completarse o si están obsoletos.

## Cómo probar la conectividad de un endpoint

- **Ping TCP (solo red)**: en un CloudShell VPC environment, ver
  `PRUEBA-VPC-ENVIRONMENT.md`. Ej:
  ```bash
  timeout 5 bash -c "</dev/tcp/MEBUC.secad.prod.mdp.local/1521" && echo OK || echo NO
  ```
- **Prueba real (red + credenciales + CDC)**: `test_endpoint_dms.py --endpoint <Id>`
  desde CloudShell normal (DMS ejecuta la prueba). Ej:
  ```bash
  python3 test_endpoint_dms.py --endpoint bucaramanga-secad-source
  ```
