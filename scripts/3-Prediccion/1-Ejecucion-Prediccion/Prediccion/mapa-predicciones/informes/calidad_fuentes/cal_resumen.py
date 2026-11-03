import json, csv, sys
d = json.load(open('cal_perfil.json', encoding='utf-8'))
NUM = lambda r, c, cols: int(r[cols.index(c)] or 0)
resumen = {}
det = []
for k, v in d.items():
    if 'cols' not in v:
        print('##', k, 'ERROR', v.get('error', '')[:300]); continue
    cols = v['cols']
    agg = {c: 0 for c in cols[1:]}
    for r in v['filas']:
        for c in cols[1:]:
            agg[c] += NUM(r, c, cols)
        fila = {'fuente': k, 'validador': v['validador'], 'grupo': r[0]}
        fila.update({c: NUM(r, c, cols) for c in cols[1:]})
        fila['duplicados'] = fila['n'] - fila['id_nulo'] - fila['ids']
        det.append(fila)
    agg['duplicados'] = sum(f['duplicados'] for f in det if f['fuente'] == k)
    resumen[k] = {'validador': v['validador'], 'origen': v['origen'], 'agg': agg, 'completa': v.get('completa'), 'extras': v['extras'], 'grupos': len(v['filas'])}
    n = agg['n'] or 1
    print(f"== {k} ({v['validador']}) filas={agg['n']:,} grupos={len(v['filas'])} completa={v.get('completa')}")
    print('   ' + ' '.join(f"{c}={agg[c]:,}({100*agg[c]/n:.1f}%)" for c in cols[2:] if c not in ('ids',) and agg[c]) + f" dup={agg['duplicados']:,}")
json.dump(resumen, open('cal_resumen.json', 'w', encoding='utf-8'), indent=1, ensure_ascii=False)
campos = sorted({c for f in det for c in f}, key=lambda c: (c not in ('fuente', 'validador', 'grupo'), c))
with open('cal_detalle.csv', 'w', newline='', encoding='utf-8-sig') as fh:
    w = csv.DictWriter(fh, campos); w.writeheader(); w.writerows(det)
