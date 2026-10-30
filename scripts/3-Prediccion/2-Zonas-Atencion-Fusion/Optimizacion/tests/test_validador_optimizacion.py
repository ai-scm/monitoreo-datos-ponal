"""Pruebas sin AWS, credenciales ni boto3; casos de entrega parcial e integridad."""
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('optimizacion',ROOT/'validador_optimizacion_cloudshell.py')
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
NOW=datetime(2026,10,29,20,0,tzinfo=timezone.utc)
WEEK='20261026';CAD='MEPER';UNIT='51716'
PREFIX=f'PrediccionMdP/2026/{WEEK}/{CAD}/{UNIT}/Optimizacion/'


class SDKError(Exception):
    def __init__(self,code='AccessDenied'):
        self.response={'Error':{'Code':code}}


def geo(index=1,cad=CAD,unit=UNIT,value=255):
    ring=[[-74.1,4.5],[-74.0,4.5],[-74.0,4.6],[-74.1,4.5]]
    return {'type':'FeatureCollection','features':[
        {'type':'Feature','properties':{'id_unidadbase':unit,'CAD':cad,
            'intensity_val':value,'sum_delito_area':2.5},
         'geometry':{'type':'Polygon','coordinates':[ring]}} for _ in range(index)]}


def object_(date='20261026',turn='2',kind='B',index=1,unit=UNIT,prefix=PREFIX):
    name=f'Optimizacion_{unit}_{date}_{turn}_F_{kind}{index:02d}.geojson'
    raw=json.dumps(geo(index,cad=prefix.split('/')[3],unit=unit)).encode()
    return dict(Key=prefix+name,Size=len(raw),LastModified=NOW,ETag='"'+name+'"'),raw


def dataset(indices=(1,),week=WEEK,unit=UNIT,cad=CAD,profiles=None):
    objects=[];bodies={}
    prefix=f'PrediccionMdP/{week[:4]}/{week}/{cad}/{unit}/Optimizacion/'
    for date,turn in sorted(v.calendario(week)):
        selected=profiles[v.slot_key(week,date,turn)][0] if profiles else indices
        for index in selected:
            for kind in ('B','CP'):
                obj,raw=object_(date,turn,kind,index,unit,prefix)
                objects.append(obj);bodies[obj['Key']]=raw
    return objects,bodies


class FakeS3:
    def __init__(self,objects=(),bodies=None,pages=None):
        self.objects=list(objects);self.bodies=bodies or {};self.pages=pages
        self.calls=[];self.gets=[];self.streams=[];self.denied={};self.uploads={}
        self.response_override={}

    def get_paginator(self,name):
        assert name=='list_objects_v2'
        return self

    def paginate(self,**kw):
        assert kw['ExpectedBucketOwner']==v.OWNER
        self.calls.append(kw['Prefix'])
        if self.pages is not None:
            for page in self.pages.get(kw['Prefix'],[{}]):
                if isinstance(page,Exception):raise page
                yield page
        else:
            yield {'Contents':[o for o in self.objects if o['Key'].startswith(kw['Prefix'])]}

    def get_object(self,**kw):
        assert kw['ExpectedBucketOwner']==v.OWNER
        self.gets.append(kw)
        key=kw['Key']
        if key in self.denied:raise SDKError(self.denied[key])
        obj=next(o for o in self.objects if o['Key']==key)
        assert kw['IfMatch']==obj['ETag']
        stream=io.BytesIO(self.bodies[key]);self.streams.append(stream)
        return dict(Body=stream,ETag=obj['ETag'],ContentLength=obj['Size'],
            LastModified=obj['LastModified'],**{}) | self.response_override

    def upload_fileobj(self,body,bucket,key,ExtraArgs):
        assert bucket==v.BUCKET and ExtraArgs['ExpectedBucketOwner']==v.OWNER
        self.uploads[key]=body.read()


def args(**kw):
    values=dict(solo_metadata=False,sin_cache=False,max_mib=8,ciclo_cerrado=False,
        sigla=CAD,unidad=UNIT,desde=WEEK,hasta=WEEK,trabajadores=2,reportes_s3=v.REPORTS)
    values.update(kw);return SimpleNamespace(**values)


def reader(s3,reads=1000):
    return v.Reader(s3,v.Budget(300,reads),(SDKError,))


class CalendarMetadataTests(unittest.TestCase):
    def setUp(self):
        self.reference,_=v.load_reference(ROOT/'referencia_optimizacion.json')

    def test_turn1_starts_sunday_and_new_year_is_supported(self):
        slots=v.calendario(WEEK)
        self.assertEqual(len(slots),21)
        self.assertIn(('20261025','1'),slots)
        self.assertNotIn(('20261101','1'),slots)
        self.assertIn(('20261101','2'),slots)
        self.assertIn(('20270103','3'),v.calendario('20261228'))

    def test_actual_week_uses_bogota(self):
        moment=datetime(2026,10,26,2,tzinfo=timezone.utc)
        self.assertEqual(v.semana_actual(moment),'20261019')

    def test_default_arguments_cover_current_and_next_week_only(self):
        with patch.object(v,'semana_actual',return_value=WEEK):
            parsed=v.arguments([])
        self.assertEqual(parsed.desde,WEEK)
        self.assertEqual(parsed.hasta,'20261102')
        self.assertFalse(parsed.hasta_explicita)

    def test_default_next_week_handles_year_change(self):
        parsed=v.arguments(['--desde','20261228'])
        self.assertEqual(parsed.hasta,'20270104')

    def test_non_monday_and_unsafe_report_destination_are_rejected(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):v.arguments(['--desde','20261025'])
            with self.assertRaises(SystemExit):v.arguments(['--reportes-s3','s3://pon-prod-ml-iapol/PrediccionMdP/'])

    def test_complete_pairs_have_no_extension_false_missing(self):
        objects,_=dataset()
        summary,checks,rows=v.metadata(objects,WEEK,CAD,UNIT,True,self.reference[CAD+'/'+UNIT])
        self.assertEqual(summary['nombres_validos'],42)
        self.assertEqual(summary['turnos_observados'],21)
        self.assertEqual(summary['pares_completos'],21)
        self.assertEqual(summary['pares_incompletos'],0)
        self.assertFalse(checks)

    def test_shifted_and_nonconsecutive_indices_are_accepted(self):
        cases=[('DECAQ','51606',tuple(range(4,15))),('METUN','51162',tuple(range(16,27))),
            ('DEBOY','52597',(1,2,3,4,5,6,8)),('MEVAL','51047',tuple(range(1,7)))]
        for cad,unit,indices in cases:
            with self.subTest(cad=cad,unit=unit):
                objects,_=dataset(indices=indices,unit=unit,cad=cad)
                summary,checks,rows=v.metadata(objects,WEEK,cad,unit,True,self.reference[cad+'/'+unit])
                self.assertEqual(summary['nombres_validos'],42*len(indices))
                self.assertEqual(summary['pares_incompletos'],0)
                self.assertFalse(checks)

    def test_mebuc_variation_is_allowed_by_observed_reference(self):
        profile=self.reference['MEBUC/60760']
        current={key:[min(options,key=len)] for key,options in profile.items()}
        objects,_=dataset(unit='60760',cad='MEBUC',profiles=current)
        summary,checks,_=v.metadata(objects,WEEK,'MEBUC','60760',True,profile)
        self.assertEqual(summary['archivos'],298)
        self.assertFalse(checks)

    def test_unpaired_and_absent_turn_are_pending_until_delivery_confirmed(self):
        objects,_=dataset();objects.pop()
        for closed,expected in ((False,'PENDIENTE'),(True,'FALLA')):
            _,checks,_=v.metadata(objects,WEEK,CAD,UNIT,True,closed=closed)
            self.assertEqual(next(c['estado'] for c in checks if c['control']=='Pareja B/CP'),expected)
        objects=[o for o in objects if '_20261025_1_' not in o['Key']]
        _,checks,_=v.metadata(objects,WEEK,CAD,UNIT,True,closed=True)
        self.assertEqual(next(c['estado'] for c in checks if c['control']=='Cobertura por turnos'),'FALLA')

    def test_empty_unit_is_not_a_proven_failure_even_after_cycle(self):
        summary,checks,_=v.metadata([],WEEK,CAD,UNIT,True,closed=True)
        self.assertEqual(summary['archivos'],0)
        self.assertNotIn('FALLA',{c['estado'] for c in checks})
        self.assertNotIn('Cobertura por turnos',{c['control'] for c in checks})

    def test_partial_list_does_not_invent_missing_turns(self):
        objects,_=dataset();objects=objects[:3]
        _,checks,_=v.metadata(objects,WEEK,CAD,UNIT,False,closed=True)
        self.assertTrue(all(c['estado']=='INCOMPLETO' for c in checks if c['control'] in ('Cobertura por turnos','Pareja B/CP')))

    def test_zero_wrong_unit_and_semantic_duplicates_are_failures(self):
        obj,_=object_();zero=dict(obj,Size=0)
        _,_,rows=v.metadata([zero],WEEK,CAD,UNIT,True)
        self.assertEqual(rows[0]['estado'],'FALLA')
        wrong,_=object_(unit='123')
        _,_,rows=v.metadata([wrong],WEEK,CAD,UNIT,True)
        self.assertEqual(rows[0]['estado'],'FALLA')
        alias=dict(obj,Key=obj['Key'].replace('_B01.','_B001.'))
        _,_,rows=v.metadata([obj,alias],WEEK,CAD,UNIT,True)
        self.assertTrue(all(r['estado']=='FALLA' for r in rows))

    def test_incomplete_and_invalid_names_are_counted_and_rejected(self):
        base,_=object_()
        names=(
            'Optimizacion_51716_20261026_2_F_B.geojson',
            'Optimizacion_51716_20261026_2_F_B1.geojson',
            'Optimizacion_51716_20261026_2_F_B01',
            'Optimizacion_51716_20261026_2_F_.geojson',
            'Optimizacion_51716_20261026_F_B01.geojson',
            'Optimizacion_20261026_2_F_B01.geojson',
            'Optimizacion_51716_20261026_4_F_B01.geojson',
            'Optimizacion_51716_20261026_2_F_B00.geojson',
            'Optimizacion_51716_20261301_2_F_B01.geojson',
            'Optimizacion_51716_20261109_2_F_B01.geojson',
            'Optimizacion_99999_20261026_2_F_B01.geojson',
            'Optimizacion_51716_20261026_2_F_B01.geojson.part')
        objects=[dict(base,Key=PREFIX+n) for n in names]
        summary,_,rows=v.metadata(objects,WEEK,CAD,UNIT,True)
        self.assertEqual(summary['nombres_invalidos'],len(names))
        self.assertEqual(summary['nombres_validos'],0)
        self.assertTrue(all(r['nombre_estado']=='FALLA' and r['estado']=='FALLA' for r in rows))

    def test_zero_bytes_are_distinct_from_naming_errors(self):
        obj,_=object_()
        summary,_,rows=v.metadata([dict(obj,Size=0)],WEEK,CAD,UNIT,True)
        self.assertEqual(summary['nombres_invalidos'],0)
        self.assertEqual(summary['nombres_validos'],1)
        self.assertEqual(summary['archivos_vacios'],1)
        self.assertEqual(rows[0]['nombre_estado'],'OK')
        self.assertEqual(rows[0]['estado'],'FALLA')

    def test_baseline_change_warns_without_inventing_business_failure(self):
        objects,_=dataset(indices=(1,2))
        _,checks,_=v.metadata(objects,WEEK,CAD,UNIT,True,self.reference[CAD+'/'+UNIT],True)
        self.assertEqual(next(c['estado'] for c in checks if c['control']=='Variantes frente a referencia'),'AVISO')

    def test_markers_are_not_prediction_files(self):
        obj=dict(Key=PREFIX+'_SUCCESS',Size=0,LastModified=NOW,ETag='x')
        summary,checks,rows=v.metadata([obj],WEEK,CAD,UNIT,True)
        self.assertEqual(summary['archivos'],0)
        self.assertFalse(rows)


class GeoJSONTests(unittest.TestCase):
    def inspect(self,data,index=1):
        return v.inspect_geojson(json.dumps(data).encode(),CAD,UNIT,index)

    def test_valid_content_and_non_probability_scale(self):
        self.assertEqual(self.inspect(geo(value=255))['estado'],'OK')
        self.assertEqual(self.inspect(geo(value=250.5))['estado'],'OK')

    def test_optional_null_fields_are_allowed(self):
        data=geo();data['features'][0]['properties'].update(intersection_area=None,intersection_percentage=None)
        self.assertEqual(self.inspect(data)['estado'],'OK')

    def test_invalid_json_duplicates_nonfinite_or_wrong_root(self):
        for raw in (b'{',b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":1e999}',b'[]',b'\xff'):
            with self.subTest(raw=raw):
                self.assertEqual(v.inspect_geojson(raw,CAD,UNIT,1)['estado'],'FALLA')

    def test_wrong_cad_and_unit_are_failures(self):
        self.assertEqual(self.inspect(geo(cad='OTRA'))['estado'],'FALLA')
        self.assertEqual(self.inspect(geo(unit='1'))['estado'],'FALLA')
        data=geo();data['features'][0]['properties']['id_unidadbase']=int(UNIT)
        self.assertEqual(self.inspect(data)['estado'],'OK')

    def test_bad_ring_and_coordinates_are_failures(self):
        for point in ([181,0],[0,91],[False,4],['a',4]):
            data=geo();data['features'][0]['geometry']['coordinates'][0][1]=point
            self.assertEqual(self.inspect(data)['estado'],'FALLA')
        data=geo();data['features'][0]['geometry']['coordinates'][0].pop()
        self.assertEqual(self.inspect(data)['estado'],'FALLA')

    def test_null_empty_and_count_mismatch_are_not_proven_corruption(self):
        data=geo();data['features'][0]['geometry']=None
        self.assertEqual(self.inspect(data)['estado'],'AVISO')
        self.assertEqual(self.inspect({'type':'FeatureCollection','features':[]})['estado'],'AVISO')
        self.assertEqual(self.inspect(geo(2),1)['estado'],'AVISO')

    def test_unknown_geometry_preserves_incomplete_coverage(self):
        data=geo();data['features'][0]['geometry']={'type':'Point','coordinates':[-74,4]}
        self.assertEqual(self.inspect(data)['contenido_estado'],'NO EVALUADO')
        self.assertEqual(self.inspect(data)['estado'],'INCOMPLETO')

    def test_multipolygon_is_supported(self):
        data=geo();g=data['features'][0]['geometry']
        g['type']='MultiPolygon';g['coordinates']=[g['coordinates']]
        self.assertEqual(self.inspect(data)['estado'],'OK')

    def test_negative_and_missing_fields_warn_without_arbitrary_threshold(self):
        self.assertEqual(self.inspect(geo(value=-1))['estado'],'AVISO')
        data=geo();data['features'][0]['properties'].pop('intensity_val')
        self.assertEqual(self.inspect(data)['estado'],'AVISO')


class IOAndResumeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.objects,self.bodies=dataset();self.s3=FakeS3(self.objects,self.bodies)
        self.reference,_=v.load_reference(ROOT/'referencia_optimizacion.json')

    def tearDown(self):self.tmp.cleanup()

    def inspect(self,s3=None,reads=1000,options=None):
        return v.inspect_unit(reader(s3 or self.s3,reads), (WEEK,CAD,UNIT),options or args(),self.root,self.reference)

    def test_second_run_uses_cache_and_only_changed_file_is_reloaded(self):
        summary,_,_=self.inspect()
        self.assertEqual(summary['contenido_ok'],42)
        self.assertEqual(summary['lecturas_nuevas'],42)
        summary,_,_=self.inspect()
        self.assertEqual(summary['lecturas_cache'],42)
        self.assertEqual(summary['lecturas_nuevas'],0)
        self.objects[0]['ETag']='"new"'
        summary,_,_=self.inspect()
        self.assertEqual(summary['lecturas_nuevas'],1)
        self.assertEqual(summary['lecturas_cache'],41)

    def test_new_read_budget_can_resume(self):
        summary,_,_=self.inspect(reads=1)
        self.assertEqual(summary['contenido_ok'],1)
        self.assertEqual(summary['contenido_pendiente'],41)
        self.assertEqual(summary['estado'],'INCOMPLETO')
        summary,_,_=self.inspect()
        self.assertEqual(summary['lecturas_cache'],1)
        self.assertEqual(summary['lecturas_nuevas'],41)
        self.assertEqual(summary['estado'],'OK')

    def test_pending_units_have_priority_over_completed_cache(self):
        candidate=(WEEK,CAD,UNIT)
        self.assertEqual(v.candidate_priority(candidate,self.root,False)[0],0)
        self.inspect(reads=1)
        self.assertEqual(v.candidate_priority(candidate,self.root,False)[0],0)
        self.inspect()
        self.assertEqual(v.candidate_priority(candidate,self.root,False)[0],1)

    def test_access_denied_is_not_zero_or_healthy_and_streams_close(self):
        self.s3.denied[self.objects[0]['Key']]='AccessDenied'
        summary,controls,rows=self.inspect()
        self.assertEqual(summary['contenido_pendiente'],1)
        self.assertIn('AccessDenied',rows[0]['detalle'])
        self.assertTrue(all(body.closed for body in self.s3.streams))

    def test_changed_object_and_short_read_are_not_cached_as_valid(self):
        for override in ({'ETag':'"changed"'},{'ContentLength':1}):
            obj=self.objects[0];self.s3.response_override=override
            with self.assertRaises(v.ReadFailure):reader(self.s3).get(obj,10**6)
            self.assertTrue(self.s3.streams[-1].closed)
        self.s3.response_override={};self.s3.bodies[self.objects[0]['Key']]=b'{' 
        with self.assertRaises(v.ReadFailure):reader(self.s3).get(self.objects[0],10**6)
        self.assertTrue(self.s3.streams[-1].closed)

    def test_oversized_files_are_not_downloaded(self):
        self.objects[0]['Size']=9*1024**2
        summary,_,_=self.inspect()
        self.assertEqual(len(self.s3.gets),41)
        self.assertEqual(summary['contenido_pendiente'],1)

    def test_partial_list_does_not_hide_known_corrupt_file(self):
        self.objects[0]['Size']=0
        self.s3.pages={PREFIX:[{'Contents':[self.objects[0]]},SDKError()]}
        summary,controls,rows=self.inspect()
        self.assertEqual(summary['estado'],'FALLA')
        self.assertEqual(summary['cobertura_listado'],'INCOMPLETO')
        self.assertIn('INCOMPLETO',{c['estado'] for c in controls})

    def test_corrupted_cache_is_rejected(self):
        path=self.root/WEEK/CAD/(UNIT+'.json');path.parent.mkdir(parents=True)
        path.write_text('[]')
        summary,controls,_=self.inspect()
        self.assertEqual(summary['contenido_ok'],42)
        self.assertTrue(any(c['control']=='Cache local' for c in controls))

    def test_metadata_only_never_claims_content_ok(self):
        summary,_,_=self.inspect(options=args(solo_metadata=True))
        self.assertEqual(summary['contenido_ok'],0)
        self.assertEqual(summary['contenido_pendiente'],42)
        self.assertFalse(self.s3.gets)

    def test_empty_file_is_failed_not_approved_or_pending(self):
        obj=self.objects[0]
        s3=FakeS3([dict(obj,Size=0)])
        summary,_,rows=self.inspect(s3=s3)
        self.assertEqual(summary['estado'],'FALLA')
        self.assertEqual(summary['archivos_vacios'],1)
        self.assertEqual(summary['contenido_pendiente'],0)
        self.assertEqual(summary['contenido_ok'],0)
        self.assertEqual(rows[0]['origen_revision'],'ARCHIVO_VACIO')
        self.assertFalse(s3.gets)

    def test_upload_uses_execution_date_and_only_two_csv(self):
        folder=self.root/'2026-10-29'/'15-30-00_000001';folder.mkdir(parents=True)
        for name in ('general.csv','detalle.csv'):(folder/name).write_text('header\nvalue\n')
        (folder/'contexto.json').write_text('{}')
        destination=v.upload(self.s3,args(),folder)
        self.assertIn('/2026-10-29/15-30-00_000001/',destination)
        self.assertEqual(len(self.s3.uploads),2)
        self.assertTrue(all(k.startswith('ValidationReports/08-MonitorOptimizacion/2026-10-29/') for k in self.s3.uploads))

    def test_discovery_cross_year_and_reports(self):
        self.s3.pages={
            'PrediccionMdP/':[{'CommonPrefixes':[{'Prefix':'PrediccionMdP/'+y+'/'} for y in ('2025','2026','2027')]}],
            'PrediccionMdP/2026/':[{'CommonPrefixes':[{'Prefix':'PrediccionMdP/2026/'+w+'/'} for w in ('20261019',WEEK)]}],
            'PrediccionMdP/2026/'+WEEK+'/':[{'CommonPrefixes':[{'Prefix':f'PrediccionMdP/2026/{WEEK}/{CAD}/'}]}],
            f'PrediccionMdP/2026/{WEEK}/{CAD}/':[{'CommonPrefixes':[{'Prefix':f'PrediccionMdP/2026/{WEEK}/{CAD}/{UNIT}/'}]}],
            PREFIX:[{'Contents':self.objects}]}
        folder=self.root/'reports';folder.mkdir()
        summaries,context,code=v.run(reader(self.s3),args(),folder,self.root/'cache',self.reference)
        self.assertEqual(code,0)
        self.assertEqual(len(summaries),1)
        self.assertFalse(any(p.startswith('PrediccionMdP/2025/') for p in self.s3.calls))
        for name in ('general.csv','detalle.csv'):
            with (folder/name).open(encoding='utf-8-sig') as f:self.assertTrue(list(csv_reader(f)))

    def test_discovery_failure_produces_incomplete_not_success(self):
        self.s3.pages={'PrediccionMdP/':[SDKError()]}
        folder=self.root/'reports';folder.mkdir()
        summaries,context,code=v.run(reader(self.s3),args(),folder,self.root/'cache',{})
        self.assertEqual(code,3)
        self.assertFalse(summaries)
        self.assertTrue((folder/'detalle.csv').exists())

    def weekly_pages(self):
        weeks=[(datetime(2026,10,26)+timedelta(weeks=i)).strftime('%Y%m%d') for i in range(10)]
        pages={
            'PrediccionMdP/':[{'CommonPrefixes':[{'Prefix':'PrediccionMdP/2026/'}]}],
            'PrediccionMdP/2026/':[{'CommonPrefixes':[{'Prefix':f'PrediccionMdP/2026/{w}/'} for w in weeks]}],
        }
        for i,week in enumerate(weeks):
            cad=CAD if i<2 else 'OTRA'
            prefix=f'PrediccionMdP/2026/{week}/'
            pages[prefix]=[{'CommonPrefixes':[{'Prefix':prefix+cad+'/'}]}]
            pages[prefix+cad+'/']=[{'CommonPrefixes':[{'Prefix':prefix+cad+'/'+UNIT+'/'}]}]
        return weeks,pages

    def test_automatic_future_does_not_invent_requested_sigla_folders(self):
        weeks,pages=self.weekly_pages();self.s3.pages=pages
        candidates,controls,_=v.discover(reader(self.s3),args(hasta=None))
        self.assertEqual(candidates,[(weeks[0],CAD,UNIT),(weeks[1],CAD,UNIT)])
        self.assertFalse(any(c['estado']=='AVISO' for c in controls))
        self.assertFalse(any(c['control']=='Semanas futuras sin carpeta seleccionada' for c in controls))
        self.assertFalse(any(f'/{w}/' in p for w in weeks[2:] for p in self.s3.calls))

    def test_automatic_future_does_not_invent_requested_unit_folder(self):
        weeks,pages=self.weekly_pages()
        prefix=f'PrediccionMdP/2026/{weeks[1]}/{CAD}/'
        pages[prefix]=[{'CommonPrefixes':[{'Prefix':prefix+'99999/'}]}]
        self.s3.pages=pages
        candidates,controls,_=v.discover(reader(self.s3),args(hasta=None))
        self.assertEqual(candidates,[(WEEK,CAD,UNIT)])
        self.assertFalse(any(c['estado']=='AVISO' for c in controls))
        self.assertTrue(any(c['estado']=='PENDIENTE' for c in controls))

    def test_next_week_without_a_folder_is_explicitly_pending(self):
        weeks,pages=self.weekly_pages()
        pages['PrediccionMdP/2026/']=[{'CommonPrefixes':[{'Prefix':f'PrediccionMdP/2026/{WEEK}/'}]}]
        self.s3.pages=pages
        _,controls,observed=v.discover(reader(self.s3),args(hasta=None))
        self.assertEqual(observed,[WEEK])
        self.assertTrue(any(c['estado']=='PENDIENTE' and c['semana']==weeks[1] and c['control']=='Semana siguiente' for c in controls))

    def test_explicit_range_retains_requested_absence_warnings(self):
        weeks,pages=self.weekly_pages();self.s3.pages=pages
        candidates,controls,_=v.discover(reader(self.s3),args(hasta=weeks[-1]))
        self.assertEqual(len(candidates),10)
        absent=[c for c in controls if c['control']=='Sigla solicitada']
        self.assertEqual({c['semana'] for c in absent},set(weeks[2:]))
        self.assertTrue(all(c['estado']=='AVISO' for c in absent))

    def test_initial_week_absence_is_not_silently_skipped(self):
        weeks,pages=self.weekly_pages()
        pages[f'PrediccionMdP/2026/{WEEK}/']=[{'CommonPrefixes':[{'Prefix':f'PrediccionMdP/2026/{WEEK}/OTRA/'}]}]
        self.s3.pages=pages
        candidates,controls,_=v.discover(reader(self.s3),args(hasta=None))
        self.assertIn((WEEK,CAD,UNIT),candidates)
        self.assertTrue(any(c['control']=='Sigla solicitada' and c['semana']==WEEK and c['estado']=='AVISO' for c in controls))

    def test_user_84_files_case_ignores_8_empty_legacy_cache_entries(self):
        weeks,pages=self.weekly_pages();objects=[];bodies={}
        for week in weeks[:2]:
            os,bs=dataset(week=week);objects.extend(os);bodies.update(bs)
            prefix=f'PrediccionMdP/2026/{week}/{CAD}/{UNIT}/Optimizacion/'
            pages[prefix]=[{'Contents':os}]
        cache=self.root/'cache'
        for week in weeks[2:]:
            path=cache/week/CAD/(UNIT+'.json')
            v.atomic_json(path,dict(policy=v.POLICY,bucket=v.BUCKET,archivos={},revision_completa=True,solo_metadata=False))
        s3=FakeS3(objects,bodies,pages)
        for i in range(2):
            folder=self.root/f'run{i}';folder.mkdir()
            summaries,context,code=v.run(reader(s3),args(hasta=None),folder,cache,self.reference)
            self.assertEqual(code,0)
            self.assertEqual(len(summaries),2)
            self.assertEqual(sum(r['archivos'] for r in summaries),84)
            self.assertEqual(sum(r['contenido_ok'] for r in summaries),84)
            self.assertEqual({r['estado'] for r in summaries},{'OK'})
            self.assertFalse(any(c['estado']=='AVISO' for c in context['controles_generales']))
            if i:self.assertEqual(sum(r['lecturas_cache'] for r in summaries),84)
        self.assertEqual(len(s3.gets),84)
        self.assertFalse(any(f'/{w}/{CAD}/{UNIT}/Optimizacion/' in p for w in weeks[2:] for p in s3.calls))

    def test_observed_future_unit_remains_visible_if_later_deleted(self):
        weeks,pages=self.weekly_pages();future=weeks[1]
        objects,bodies=dataset(week=future)
        s3=FakeS3(objects,bodies)
        v.inspect_unit(reader(s3),(future,CAD,UNIT),args(hasta=None),self.root,self.reference)
        pages[f'PrediccionMdP/2026/{future}/']=[{'CommonPrefixes':[{'Prefix':f'PrediccionMdP/2026/{future}/OTRA/'}]}]
        current_objects,current_bodies=dataset()
        pages[PREFIX]=[{'Contents':current_objects}]
        s3=FakeS3(current_objects,current_bodies,pages)
        for i in range(2):
            folder=self.root/f'deleted{i}';folder.mkdir()
            summaries,_,_=v.run(reader(s3),args(hasta=None),folder,self.root,self.reference)
            future_row=next(r for r in summaries if r['semana']==future)
            self.assertEqual(future_row['archivos'],0)
            self.assertEqual(future_row['estado'],'AVISO')
            cache=json.loads((self.root/future/CAD/(UNIT+'.json')).read_text())
            self.assertTrue(cache['tuvo_archivos'])

    def test_future_permission_error_is_not_a_benign_absence(self):
        weeks,pages=self.weekly_pages();future=weeks[1]
        pages[f'PrediccionMdP/2026/{future}/']=[SDKError()]
        pages[PREFIX]=[{'Contents':self.objects}]
        s3=FakeS3(self.objects,self.bodies,pages)
        folder=self.root/'permission';folder.mkdir()
        _,context,code=v.run(reader(s3),args(hasta=None),folder,self.root/'cache',self.reference)
        self.assertEqual(code,3)
        self.assertTrue(any(c['estado']=='INCOMPLETO' and 'AccessDenied' in c['detalle'] for c in context['controles_generales']))

    def test_future_cache_with_observed_files_cannot_expand_default_two_weeks(self):
        weeks,pages=self.weekly_pages();cache=self.root/'cache'
        for week in weeks[2:]:
            v.atomic_json(cache/week/CAD/(UNIT+'.json'),dict(policy=v.POLICY,bucket=v.BUCKET,
                archivos={},tuvo_archivos=True,revision_completa=True,solo_metadata=False))
        objects=[];bodies={}
        for week in weeks[:2]:
            os,bs=dataset(week=week);objects.extend(os);bodies.update(bs)
            pages[f'PrediccionMdP/2026/{week}/{CAD}/{UNIT}/Optimizacion/']=[{'Contents':os}]
        s3=FakeS3(objects,bodies,pages)
        folder=self.root/'two-weeks';folder.mkdir()
        summaries,context,code=v.run(reader(s3),args(hasta=None),folder,cache,self.reference)
        self.assertEqual(code,0)
        self.assertEqual(context['hasta'],weeks[1])
        self.assertEqual(context['semanas'],weeks[:2])
        self.assertEqual({r['semana'] for r in summaries},set(weeks[:2]))
        self.assertFalse(any(f'/{w}/' in p for w in weeks[2:] for p in s3.calls))

    def test_csv_includes_two_cad_weeks_with_expected_found_and_invalid_names(self):
        weeks,pages=self.weekly_pages();objects=[];bodies={}
        for week in weeks[:2]:
            os,bs=dataset(week=week)
            if week==WEEK:
                os.append(dict(os[0],Key=PREFIX+'Optimizacion_51716_20261026_2_F_B.geojson'))
            objects.extend(os);bodies.update(bs)
            pages[f'PrediccionMdP/2026/{week}/{CAD}/{UNIT}/Optimizacion/']=[{'Contents':os}]
        s3=FakeS3(objects,bodies,pages)
        folder=self.root/'csv-summary';folder.mkdir()
        _,context,code=v.run(reader(s3),args(hasta=None),folder,self.root/'cache',self.reference)
        with (folder/'general.csv').open(encoding='utf-8-sig') as f:
            records=list(csv_reader(f))
        reports=[r for r in records if r['tipo_registro']=='RESUMEN_SIGLA']
        self.assertEqual(len(reports),2)
        by_week={r['semana']:r for r in reports}
        for row in reports:
            self.assertEqual(row['archivos_esperados_min'],'42')
            self.assertEqual(row['archivos_esperados_max'],'42')
        self.assertEqual(by_week[WEEK]['archivos'],'43')
        self.assertEqual(by_week[WEEK]['nombres_invalidos'],'1')
        self.assertEqual(by_week[WEEK]['estado'],'FALLA')
        self.assertEqual(by_week[weeks[1]]['archivos'],'42')
        self.assertEqual(by_week[weeks[1]]['nombres_invalidos'],'0')
        self.assertEqual(by_week[weeks[1]]['comparacion_referencia'],'COINCIDE CON REFERENCIA')
        self.assertEqual(len(context['resumen_siglas']),2)
        self.assertEqual(code,1)
        self.assertEqual(len(s3.gets),84)

    def two_week_data(self):
        weeks,pages=self.weekly_pages();objects=[];bodies={}
        for week in weeks[:2]:
            os,bs=dataset(week=week);objects.extend(os);bodies.update(bs)
            pages[f'PrediccionMdP/2026/{week}/{CAD}/{UNIT}/Optimizacion/']=[{'Contents':os}]
        return FakeS3(objects,bodies,pages)

    def run_two_weeks(self,s3,live_reader=None,name='national',options=None):
        folder=self.root/name;folder.mkdir()
        with redirect_stdout(io.StringIO()):
            return v.run(live_reader or reader(s3),options or args(hasta=None),folder,
                         self.root/'cache',self.reference)

    def test_all_unit_lists_finish_before_first_content_get(self):
        s3=self.two_week_data();original=s3.get_object
        prefixes={f'PrediccionMdP/2026/{w}/{CAD}/{UNIT}/Optimizacion/' for w in (WEEK,'20261102')}
        def checked_get(**kwargs):
            self.assertTrue(prefixes.issubset(set(s3.calls)))
            return original(**kwargs)
        with patch.object(s3,'get_object',side_effect=checked_get):
            summaries,context,code=self.run_two_weeks(s3)
        self.assertEqual(code,0)
        self.assertEqual(context['unidades_listado_completo'],2)
        self.assertTrue(context['listado_antes_contenido'])
        self.assertEqual(sum(r['archivos'] for r in summaries),84)
        self.assertEqual(len(s3.gets),84)
        self.assertTrue(all(s3.calls.count(p)==1 for p in prefixes))

    def test_timeout_on_first_get_preserves_counts_of_both_weeks(self):
        s3=self.two_week_data();live_reader=reader(s3);original=s3.get_object
        def stop_after_first_get(**kwargs):
            response=original(**kwargs);live_reader.budget.stop();return response
        with patch.object(s3,'get_object',side_effect=stop_after_first_get):
            summaries,context,code=self.run_two_weeks(s3,live_reader,options=args(hasta=None,trabajadores=1))
        self.assertEqual(code,3)
        self.assertEqual(context['unidades_listado_completo'],2)
        self.assertEqual(sum(r['archivos'] for r in summaries),84)
        self.assertEqual(sum(r['contenido_ok'] for r in summaries),1)
        self.assertEqual(sum(r['contenido_pendiente'] for r in summaries),83)
        self.assertTrue(all(r['archivos']==42 for r in context['resumen_siglas']))

    def test_read_limit_keeps_inventory_complete_and_can_resume(self):
        s3=self.two_week_data()
        summaries,context,code=self.run_two_weeks(s3,reader(s3,reads=1))
        self.assertEqual(code,3)
        self.assertEqual(sum(r['archivos'] for r in summaries),84)
        self.assertEqual(context['unidades_listado_completo'],2)
        self.assertEqual(sum(r['contenido_pendiente'] for r in summaries),83)
        summaries,context,code=self.run_two_weeks(s3,name='resumed')
        self.assertEqual(code,0)
        self.assertEqual(sum(r['lecturas_cache'] for r in summaries),1)
        self.assertEqual(context['lecturas_nuevas'],83)
        self.assertEqual(sum(r['contenido_ok'] for r in summaries),84)

    def test_previously_validated_future_week_survives_content_timeout(self):
        s3=self.two_week_data();cache=self.root/'cache'
        v.inspect_unit(reader(s3),('20261102',CAD,UNIT),args(),cache,self.reference)
        live_reader=reader(s3);original=s3.get_object
        def stop_after_first_get(**kwargs):
            response=original(**kwargs);live_reader.budget.stop();return response
        with patch.object(s3,'get_object',side_effect=stop_after_first_get):
            summaries,context,code=self.run_two_weeks(s3,live_reader,options=args(hasta=None,trabajadores=1))
        future=next(r for r in summaries if r['semana']=='20261102')
        self.assertEqual(future['archivos'],42)
        self.assertEqual(future['contenido_ok'],42)
        self.assertEqual(future['lecturas_cache'],42)
        self.assertEqual(future['estado'],'OK')
        self.assertEqual(context['unidades_listado_completo'],2)
        self.assertEqual(code,3)

    def test_metadata_mode_preserves_cached_content_for_next_full_run(self):
        s3=self.two_week_data()
        self.run_two_weeks(s3)
        self.assertEqual(len(s3.gets),84)
        summaries,_,_=self.run_two_weeks(s3,name='metadata',options=args(hasta=None,solo_metadata=True))
        self.assertEqual(sum(r['contenido_ok'] for r in summaries),0)
        self.assertEqual(sum(r['contenido_pendiente'] for r in summaries),84)
        summaries,context,code=self.run_two_weeks(s3,name='full-again')
        self.assertEqual(code,0)
        self.assertEqual(context['lecturas_nuevas'],0)
        self.assertEqual(sum(r['lecturas_cache'] for r in summaries),84)
        self.assertEqual(len(s3.gets),84)

    def test_listing_permission_failure_remains_explicit_in_prepared_inventory(self):
        s3=self.two_week_data()
        s3.pages[f'PrediccionMdP/2026/20261102/{CAD}/{UNIT}/Optimizacion/']=[SDKError()]
        summaries,context,code=self.run_two_weeks(s3)
        future=next(r for r in summaries if r['semana']=='20261102')
        self.assertEqual(code,3)
        self.assertEqual(context['unidades_listado_completo'],1)
        self.assertEqual(future['cobertura_listado'],'INCOMPLETO')
        self.assertEqual(future['estado'],'INCOMPLETO')
        future_cad=next(r for r in context['resumen_siglas'] if r['semana']=='20261102')
        self.assertEqual(future_cad['comparacion_referencia'],'NO EVALUABLE: LISTADO INCOMPLETO')

    def test_prepared_metadata_still_checks_object_version_and_closes_body(self):
        path,complete=v.prepare_listing(reader(self.s3),(WEEK,CAD,UNIT),self.root/'inventory')
        self.assertTrue(complete)
        self.s3.response_override={'ETag':'"changed"'}
        summary,_,_=v.inspect_unit(reader(self.s3),(WEEK,CAD,UNIT),args(),self.root/'cache',self.reference,path)
        self.assertEqual(summary['contenido_ok'],0)
        self.assertEqual(summary['contenido_pendiente'],42)
        self.assertTrue(all(s.closed for s in self.s3.streams))

    def test_temporaries_are_removed_after_success_and_interruption(self):
        original_directory=v.tempfile.TemporaryDirectory;paths=[]
        def temporary(**kwargs):
            directory=original_directory(dir=self.root,**kwargs)
            paths.append(Path(directory.name));return directory
        s3=self.two_week_data()
        with patch.object(v.tempfile,'TemporaryDirectory',side_effect=temporary):
            self.run_two_weeks(s3)
            with patch.object(v,'prepare_listing',side_effect=KeyboardInterrupt):
                summaries,context,code=self.run_two_weeks(s3,name='interrupted')
        self.assertEqual(code,130)
        self.assertFalse(summaries)
        self.assertTrue(any(c['estado']=='INCOMPLETO' for c in context['controles_generales']))
        self.assertEqual(len(paths),2)
        self.assertTrue(all(not path.exists() for path in paths))

    def test_unknown_listing_count_is_displayed_as_unknown_not_empty(self):
        row=dict(semana=WEEK,sigla=CAD,unidad=UNIT,cobertura_listado='INCOMPLETO',
                 archivos=0,contenido_ok=0,contenido_con_hallazgos=0,contenido_pendiente=0,estado='INCOMPLETO')
        output=io.StringIO()
        with redirect_stdout(output):v.print_table([row])
        self.assertIn('0/1',output.getvalue())
        self.assertIn('N/V',output.getvalue())
        row['archivos']=42
        output=io.StringIO()
        with redirect_stdout(output):v.print_table([row])
        self.assertIn('>=42',output.getvalue())


class CADSummaryTests(unittest.TestCase):
    def setUp(self):
        self.reference,_=v.load_reference(ROOT/'referencia_optimizacion.json')

    def row(self,unit=UNIT,week=WEEK,cad=CAD,count=42,**kw):
        row=dict(semana=week,sigla=cad,unidad=unit,archivos=count,
                 cobertura_listado='LEIDA',estado='OK',nombres_validos=count,
                 nombres_invalidos=0,archivos_vacios=0,contenido_ok=count,contenido_pendiente=0)
        row.update(kw);return row

    def summarize(self,rows,options=None,reference=None,controls=(),candidates=None):
        if candidates is None:candidates=[(r['semana'],r['sigla'],r['unidad']) for r in rows]
        return v.summarize_cads(rows,options or args(),
            self.reference if reference is None else reference,candidates,controls)

    def test_real_meper_reference_totals_3570_for_each_week_not_48_times_42(self):
        rows=[]
        for week in (WEEK,'20261102'):
            for key,profile in self.reference.items():
                cad,unit=key.split('/')
                if cad==CAD:
                    count=2*sum(len(options[0]) for options in profile.values())
                    rows.append(self.row(unit,week,count=count))
            rows.extend(self.row('9000'+str(i),week,count=0,estado='AVISO') for i in range(5))
        reports=self.summarize(rows,args(unidad=None,hasta=None))
        self.assertEqual(len(reports),2)
        for row in reports:
            self.assertEqual(row['unidades_observadas'],48)
            self.assertEqual(row['unidades_referencia'],43)
            self.assertEqual(row['unidades_sin_referencia'],5)
            self.assertEqual(row['archivos_esperados_min'],3570)
            self.assertEqual(row['archivos_esperados_max'],3570)
            self.assertEqual(row['archivos'],3570)
            self.assertEqual(row['comparacion_referencia'],'COINCIDE CON REFERENCIA')
        self.assertEqual(sum(r['archivos'] for r in reports),7140)

    def test_different_units_and_nonconsecutive_indices_have_distinct_expectations(self):
        cases=[(CAD,UNIT,42),('MEVAL','51047',252),('DECAQ','51606',462),
               ('METUN','51162',462),('DEBOY','52597',294)]
        for cad,unit,count in cases:
            with self.subTest(cad=cad):
                row=self.summarize([self.row(unit,cad=cad,count=count)],args(sigla=cad,unidad=unit))[0]
                self.assertEqual(row['archivos_esperados_min'],count)
                self.assertEqual(row['archivos_esperados_max'],count)
                self.assertEqual(row['estado'],'OK')

    def test_mebuc_variable_count_is_a_range(self):
        for count in (298,302,306):
            row=self.summarize([self.row('60760',cad='MEBUC',count=count)],
                               args(sigla='MEBUC',unidad='60760'))[0]
            self.assertEqual((row['archivos_esperados_min'],row['archivos_esperados_max']),(298,306))
            self.assertEqual(row['comparacion_referencia'],'DENTRO DEL RANGO DE REFERENCIA')
            self.assertEqual(row['estado'],'OK')

    def test_count_change_is_notice_not_confirmed_failure_even_when_closed(self):
        for count,label in ((40,'POR DEBAJO DE REFERENCIA'),(44,'POR ENCIMA DE REFERENCIA')):
            report=self.summarize([self.row(count=count)],args(ciclo_cerrado=True))[0]
            self.assertEqual(report['comparacion_referencia'],label)
            self.assertEqual(report['estado'],'AVISO')

    def test_absent_reference_units_are_retained_in_expected_count(self):
        report=self.summarize([self.row()],args(unidad=None))[0]
        self.assertEqual(report['archivos_esperados_min'],3570)
        self.assertEqual(report['unidades_referencia_no_observadas'],42)
        self.assertEqual(report['estado'],'AVISO')

    def test_unknown_new_unit_files_do_not_claim_complete_comparison(self):
        report=self.summarize([self.row(),self.row('99999')],args(unidad=None),
                             {CAD+'/'+UNIT:self.reference[CAD+'/'+UNIT]})[0]
        self.assertEqual(report['unidades_sin_referencia'],1)
        self.assertEqual(report['archivos'],84)
        self.assertEqual(report['archivos_esperados_min'],42)
        self.assertEqual(report['comparacion_referencia'],'REFERENCIA PARCIAL')

    def test_missing_or_incomplete_reference_never_becomes_zero_expected(self):
        profile=dict(self.reference[CAD+'/'+UNIT]);profile.pop('0/1')
        for reference in ({},{CAD+'/'+UNIT:profile}):
            report=self.summarize([self.row()],reference=reference)[0]
            self.assertEqual(report['archivos_esperados_min'],'')
            self.assertEqual(report['archivos_esperados_max'],'')
            self.assertEqual(report['comparacion_referencia'],'SIN REFERENCIA COMPLETA')

    def test_partial_listing_does_not_claim_shortage_or_full_count(self):
        cases=[([self.row(count=3,cobertura_listado='INCOMPLETO',estado='INCOMPLETO')],[],None),
               ([self.row()],[],[(WEEK,CAD,UNIT),(WEEK,CAD,'99999')]),
               ([self.row()],[v.check('','','','Descubrimiento','INCOMPLETO','AccessDenied')],None)]
        for rows,controls,candidates in cases:
            report=self.summarize(rows,controls=controls,candidates=candidates)[0]
            self.assertEqual(report['cobertura_listado'],'INCOMPLETO')
            self.assertEqual(report['estado'],'INCOMPLETO')
            self.assertEqual(report['comparacion_referencia'],'NO EVALUABLE: LISTADO INCOMPLETO')

    def test_content_pending_does_not_hide_complete_filename_counts(self):
        row=self.row(contenido_ok=0,contenido_pendiente=42,estado='INCOMPLETO')
        report=self.summarize([row])[0]
        self.assertEqual(report['cobertura_listado'],'LEIDA')
        self.assertEqual(report['archivos'],42)
        self.assertEqual(report['comparacion_referencia'],'COINCIDE CON REFERENCIA')
        self.assertEqual(report['estado'],'INCOMPLETO')

    def test_next_week_without_files_is_visible_and_pending(self):
        notice=v.check('20261102','','','Semana siguiente','PENDIENTE','Todavia no hay carpeta')
        reports=self.summarize([self.row()],args(hasta=None),controls=[notice])
        future=next(r for r in reports if r['semana']=='20261102')
        self.assertEqual(future['archivos'],0)
        self.assertEqual(future['archivos_esperados_min'],42)
        self.assertEqual(future['estado'],'PENDIENTE')

    def test_no_filter_includes_all_reference_cads_and_year_crossing(self):
        reports=self.summarize([],args(sigla=None,unidad=None,desde='20261228',hasta=None))
        self.assertEqual({r['semana'] for r in reports},{'20261228','20270104'})
        self.assertEqual({r['sigla'] for r in reports},{key.split('/')[0] for key in self.reference})
        self.assertTrue(all(r['archivos_esperados_min']>0 for r in reports))


def csv_reader(handle):
    import csv
    return csv.DictReader(handle)


if __name__=='__main__':unittest.main()
