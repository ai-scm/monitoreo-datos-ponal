import boto3, time, sys
OUT='s3://aws-athena-query-results-us-east-1-926162397524/'
a=boto3.client('athena')
def q(sql, db='pon-prod-raw-secad', quiet=False):
    qid=a.start_query_execution(QueryString=sql,QueryExecutionContext={'Database':db},ResultConfiguration={'OutputLocation':OUT})['QueryExecutionId']
    while True:
        e=a.get_query_execution(QueryExecutionId=qid)['QueryExecution']
        s=e['Status']['State']
        if s in ('SUCCEEDED','FAILED','CANCELLED'): break
        time.sleep(1.5)
    if s!='SUCCEEDED':
        raise RuntimeError(e['Status'].get('StateChangeReason'))
    rows=[]; tok=None
    while True:
        kw=dict(QueryExecutionId=qid,MaxResults=1000)
        if tok: kw['NextToken']=tok
        r=a.get_query_results(**kw)
        rows+=[[c.get('VarCharValue') for c in x['Data']] for x in r['ResultSet']['Rows']]
        tok=r.get('NextToken')
        if not tok: break
    mb=e['Statistics'].get('DataScannedInBytes',0)/1e6
    if not quiet: print(f'-- {len(rows)-1} filas, {mb:.1f} MB', file=sys.stderr)
    return rows
def show(rows, w=None):
    for r in rows: print(' | '.join('' if c is None else str(c) for c in r))
