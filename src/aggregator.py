import datetime
import os
import sentry_sdk
import time
from datetime import timedelta, datetime
from influxdb_client.client.write_api import SYNCHRONOUS
import asyncio
from event_stream import dao
from event_stream.models.model import *
from influxdb_client import InfluxDBClient, BucketRetentionRules, Task
from sqlalchemy.orm import sessionmaker, scoped_session
import pandas as pd
import pymannkendall as mk
from sqlalchemy import text, bindparam
import faust
import tweepy

broker = os.environ.get('KAFKA_BOOTRSTRAP_SERVER', 'kafka:9092')
app = faust.App(
    'aggregator',
    broker=broker,
    stream_publish_on_commit=False,
    producer_max_request_size=1500000,
    topic_partitions=1,
)

SENTRY_DSN = os.environ.get('SENTRY_DSN')
SENTRY_TRACE_SAMPLE_RATE = os.environ.get('SENTRY_TRACE_SAMPLE_RATE')
sentry_sdk.init(
    dsn=SENTRY_DSN,
    traces_sample_rate=SENTRY_TRACE_SAMPLE_RATE
)

DAO = dao.DAO()

processed_topic = app.topic('events_processed-discusses')
aggregated_topic = app.topic('events_aggregated')

org = os.environ.get('INFLUXDB_V2_ORG', 'ambalytics')

client = InfluxDBClient.from_env_properties()
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

trending_time_definition = {
    'currently': {
        'name': 'currently',
        'trending_bucket': 'trending_currently',
        'time_exponent': -0.00036,
        'duration': timedelta(hours=-6),
        'retention': timedelta(hours=7),
        'window_size': timedelta(minutes=6),
        'window_count': 60,
        'min_count': 10,
        'trending_interval': timedelta(minutes=3),
        'downsample_bucket': 'today',
        'downsample_window': timedelta(minutes=2),
        'numbers_window': timedelta(minutes=1)
    },
    'today': {
        'name': 'today',
        'trending_bucket': 'trending_today',
        'time_exponent': -0.000025,
        'duration': timedelta(hours=-24),
        'retention': timedelta(hours=25),
        'window_size': timedelta(minutes=24),
        'window_count': 60,
        'min_count': 30,  # max 576
        'trending_interval': timedelta(minutes=45),
        'downsample_bucket': 'week',
        'downsample_window': timedelta(minutes=20),
        'numbers_window': timedelta(minutes=2)
    },
    'week': {
        'name': 'week',
        'trending_bucket': 'trending_week',
        'time_exponent': -0.00000357142857,
        'duration': timedelta(days=-7),
        'retention': timedelta(days=7, hours=1),
        'window_size': timedelta(minutes=168),
        'window_count': 60,
        'min_count': 30,  # max 504
        'trending_interval': timedelta(hours=6),
        'downsample_bucket': 'month',
        'downsample_window': timedelta(hours=2),
        'numbers_window': timedelta(minutes=20)
    },
    'month': {
        'name': 'month',
        'trending_bucket': 'trending_month',
        'time_exponent': -0.000000833333333,
        'duration': timedelta(days=-30),
        'retention': timedelta(days=30, hours=1),
        'window_size': timedelta(minutes=720),
        'window_count': 60,
        'min_count': 30,  # max 360
        'trending_interval': timedelta(hours=24),
        'downsample_bucket': 'year',
        'downsample_window': timedelta(hours=24),
        'numbers_window': timedelta(hours=2)
    },
    'year': {
        'name': 'year',
        'trending_bucket': 'trending_year',
        'time_exponent': -0.000000833333333,
        'duration': timedelta(days=-365),
        'retention': timedelta(days=365, hours=1),
        'window_size': timedelta(minutes=8760),
        'window_count': 60,
        'min_count': 30,  # max 365
        'trending_interval': timedelta(days=3),
        'downsample_bucket': 'history',
        'downsample_window': timedelta(days=3),
        'numbers_window': timedelta(hours=24)
    },
    'numbers': {
        'name': 'numbers',
        'retention': timedelta(days=7),
        'trending_bucket': False,
        'downsample_bucket': False
    },
    # 'history': {
    #     'name': 'history',
    #     'trending_bucket': 'trending_history',
    #     'duration': None,  # todo use date?
    #     'retention': None,
    #     'trending_interval': timedelta(hours=12),
    #     'time_exponent': -0.000000833333333,
    #     'window_size': timedelta(minutes=720),  # todo?
    #     'min_count': 15,
    #     'window_count': 60,
    #     'downsample_bucket': None,
    #     'downsample_window': None
    # }
}


@app.task
async def init_influx():
    buckets_api = client.buckets_api()
    buckets = buckets_api.find_buckets().buckets

    org_api = client.organizations_api()
    org_obj = org_api.find_organizations(org=org)[0]

    tasks_api = client.tasks_api()

    for key, item in trending_time_definition.items():
        exist = False
        for b in buckets:
            if b.name == item['name']:
                exist = True
        if not exist:
            if item['retention']:
                print('create bucket %s with retention %s' % (item['name'], str(item['retention'].total_seconds())))
                retention_rules = BucketRetentionRules(type="expire",
                                                       every_seconds=int(item['retention'].total_seconds()))
                created_bucket = buckets_api.create_bucket(bucket_name=item['name'],
                                                           retention_rules=retention_rules, org=org)
            else:
                print('create bucket %s without retention' % item['name'])
                created_bucket = buckets_api.create_bucket(bucket_name=item['name'], org=org)

            if item['trending_bucket']:
                print('create trending bucket %s without retention' % item['trending_bucket'])
                created_bucket = buckets_api.create_bucket(bucket_name=item['trending_bucket'], org=org)

            if item['downsample_bucket']:
                print('create downsample task')

                every = str(int(item['downsample_window'].total_seconds() // 60)) + "m"
                name = "task_" + item['downsample_bucket']

                flux = '''
                    import "date"
                    import "math"
                    import "experimental"
                    option task = { 
                      name: "''' + name + '''",
                      every: ''' + every + '''
                    }
                    
                '''
                flux += '_w_int = '
                flux += str(int(item['downsample_window'].total_seconds() // 60))
                flux += """
                    _window = duration(v: string(v: _w_int) + "m")
                    _duration = duration(v: string(v: 2 * _w_int) + "m")
            
                    _start = experimental.subDuration(d: _duration, from: date.truncate(t: now(), unit: _window))
                    baseTable = from(bucket: """
                flux += '"' + item['name'] + '"'
                flux += """)
                        |> range(start: _start, stop: date.truncate(t: now(), unit: _window))
                        |> filter(fn: (r) => r["_measurement"] == "trending")
                    
                        a = baseTable
                            |> filter(fn: (r) => r["_field"] == "followers")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: sum, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        b = baseTable
                            |> filter(fn: (r) => r["_field"] == "sentiment_raw")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        c = baseTable
                            |> filter(fn: (r) => r["_field"] == "contains_abstract_raw")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        ad = baseTable
                            |> filter(fn: (r) => r["_field"] == "questions")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        e = baseTable
                            |> filter(fn: (r) => r["_field"] == "exclamations")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        j = baseTable
                            |> filter(fn: (r) => r["_field"] == "length")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: count, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> set(key: "_field", value: "count")
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        f = baseTable
                            |> filter(fn: (r) => r["_field"] == "length")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        jk = baseTable
                            |> filter(fn: (r) => r["_field"] == "bot_rating")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        g = baseTable
                            |> filter(fn: (r) => r["_field"] == "score")
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> aggregateWindow(fn: sum, every: _window, createEmpty: false)
                            |> group()
                            |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                            |> keep(columns: ["_value", "_time", "_field", "doi", "_measurement"])
                        
                        union(tables: [a, b, c, ad, e, j, f, jk, g])
                            |> group(columns: ["doi"])
                            |> to(bucket: """
                flux += '"' + item['downsample_bucket'] + '"'
                flux += """, org: "ambalytics")"""
                # print(name)
                # print(org_obj.id)
                # print(flux)
                task = Task(id=0, name=name, org_id=org_obj.id, status="active", flux=flux)

                task = tasks_api.create_task(task)

                print('create numbers task')

                every = str(int(item['numbers_window'].total_seconds() // 60)) + "m"
                name = "task_numbers_" + item['name']

                flux = '''
                    import "date"
                    import "math"
                    import "experimental"
                    option task = { 
                      name: "''' + name + '''",
                      every: ''' + every + '''
                    }
                    
                '''
                flux += """
                            _start = """ + str(int(item['duration'].total_seconds())) + """s
                            _stop = now()
                            _bucket = """ + '"' + item['name'] + '"' + """
                            
                            numbers = (tables=<-, field_selector, aggregator, bucket, name) => 
                              tables
                                 |> filter(fn: (r) => r["_field"] == field_selector)
                                 |> group()
                                 |> aggregator()
                                 |> keep(columns: ["_value"])
                                 |> toFloat()
                                 |> set(key: "_measurement", value: bucket)
                                 |> set(key: "_field", value: name)
                                 |> map(fn: (r) => ({ r with _time: now() }))
                            
                            aa = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: experimental.mean, field_selector: "bot_rating", bucket: _bucket, name: "bot_rating")
                             
                            ab = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: experimental.mean, field_selector: "contains_abstract_raw", bucket: _bucket, name: "contains_abstract_raw")
                             
                            ac = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: experimental.mean, field_selector: "exclamations", bucket: _bucket, name: "exclamations")
                             
                            ad = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: sum, field_selector: "followers", bucket: _bucket, name: "followers")
                             
                            ae = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measur,,ement"] == "trending")
                              |> numbers(aggregator: experimental.mean, field_selector: "length", bucket: _bucket, name: "length")
                             
                            af = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: experimental.mean, field_selector: "questions", bucket: _bucket, name: "questions")
                             
                            ag = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: sum, field_selector: "score", bucket: _bucket, name: "score")
                             
                            ah = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: experimental.mean, field_selector: "sentiment_raw", bucket: _bucket, name: "sentiment_raw")
                             
                            ai = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> numbers(aggregator: sum, field_selector: "count", bucket: _bucket, name: "count")
                             
                            aj = from(bucket: _bucket)
                              |> range(start: _start, stop: _stop)
                              |> filter(fn: (r) => r["_measurement"] == "trending")
                              |> filter(fn: (r) => r["_field"] == "score")
                              |> group()
                              |> distinct(column: "doi")
                              |> count()
                              |> keep(columns: ["_value"])
                              |> toFloat()
                              |> set(key: "_measurement", value: _bucket)
                              |> set(key: "_field", value: "pub_count")
                              |> map(fn: (r) => ({ r with _time: now() }))
                             
                             union(tables: [aa, ab, ac, ad, ae, af, ag, ah, ai, aj])
                              |> to(bucket: "numbers", org: "ambalytics")"""
                # print(name)
                # print(org_obj.id)
                # print(flux)
                task = Task(id=1, name=name, org_id=org_obj.id, status="active", flux=flux)
                task = tasks_api.create_task(task)


# not in influx because
# need dois (-)
# need trend calc (+)
# subject to change
async def run_trend_calculation(trending_time):
    loop = asyncio.get_event_loop()
    # Using None will create a ThreadPoolExecutor
    # you can also pass in an executor (Thread or Process)
    await loop.run_in_executor(None, get_base_trend_table, trending_time)


@app.timer(interval=trending_time_definition['currently']['trending_interval'])
async def trend_calc_currently():
    """
    run trend calculation in the defined interval

    """
    print('calc trend currently')
    await run_trend_calculation(trending_time_definition['currently'])


@app.timer(interval=trending_time_definition['today']['trending_interval'])
async def trend_calc_today():
    print('calc trend today')
    await run_trend_calculation(trending_time_definition['today'])


@app.timer(interval=trending_time_definition['week']['trending_interval'])
async def trend_calc_week():
    print('calc trend week')
    await run_trend_calculation(trending_time_definition['week'])


@app.timer(interval=trending_time_definition['month']['trending_interval'])
async def trend_calc_month():
    print('calc trend month')
    await run_trend_calculation(trending_time_definition['month'])


@app.timer(interval=trending_time_definition['year']['trending_interval'])
async def trend_calc_year():
    print('calc trend year')
    await run_trend_calculation(trending_time_definition['year'])


# time is utc
@app.crontab('0 6 * * *')
async def hot_papers():
    query = """
            SELECT ROW_NUMBER() OVER (ORDER BY t.score DESC) as trending_ranking, *
            FROM (SELECT distinct on (p.id) score, p.*, a.name
                  FROM trending t
                       JOIN publication p on p.doi = t.publication_doi
                       JOIN publication_field_of_study pfos on p.doi = pfos.publication_doi
                       JOIN publication_author pa on pfos.publication_doi = pa.publication_doi
                       JOIN author a on pa.author_id = a.id
                WHERE duration = :duration AND field_of_study_id = :fos_id) t
            ORDER BY trending_ranking LIMIT 3;
    """
    duration = trending_time_definition['today']['name']
    fos_id = 21
    params = {'duration': duration, 'fos_id': fos_id}

    s = text(query).bindparams(bindparam('duration'), bindparam('fos_id'))

    session_factory = sessionmaker(bind=DAO.engine)
    Session = scoped_session(session_factory)
    session = Session()
    result = session.execute(s, params).fetchall()

    length = 40
    end = 4
    pretext = ""
    while end > 0:
        pretext = 'Todays trending papers in general medicine:'
        for r in result:
            pretext += "\n" + str(r['trending_ranking']) + '. ' + r['name'].split(' ')[-1] + ' et al. ' \
                       + smart_truncate(r['title'], length)
        pretext += '\nhttps://bit.ly/3ADeaMK'

        if len(pretext) > 280:
            length -= 10
        else:
            break
        end -= 1

    print(pretext)
    print(len(pretext))

    if 100 < len(pretext) < 280:
        consumer_key = os.environ.get('CONSUMER_KEY_TWITTER_BOT')
        consumer_secret = os.environ.get('CONSUMER_SECRET_TWITTER_BOT')
        access_token = os.environ.get('ACCESS_TOKEN_TWITTER_BOT')
        access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET_TWITTER_BOT')
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        print(api.verify_credentials().name)
        api.update_status(status=pretext)


def smart_truncate(content, length=100, suffix=' (...)'):
    if len(content) <= length:
        return content
    else:
        return ' '.join(content[:length + 1].split(' ')[0:-1]) + suffix


def get_doi_list_trending(trending):
    """
    Return a list of dois that have the required min count for the trending definition given.
    It will use the count of datapoints not the actual tweet count.

    :param trending:
    :return:
    """
    p = {"_bucket": trending['name'],
         "_min_count": trending['min_count'],
         "_start": trending['duration'],
         }
    # |> range(start: 2021-09-24T14:00:00Z, stop: 2021-09-24T19:00:00Z)
    query = """
        _stop = now()
        countTable = from(bucket: _bucket)
            |> range(start: _start, stop: _stop)
            |> filter(fn: (r) => r["_measurement"] == "trending")
            |> filter(fn: (r) => r["_field"] == "score")
            |> count()
            |> filter(fn: (r) => r["_value"] > _min_count)
            |> group()
            |> keep(columns: ["doi"])
            |> yield()  
    """

    result = query_api.query(org=org, query=query, params=p)
    results = []
    for table in result:
        for record in table.records:
            results.append(record['doi'])
    # print(results)
    return results


def calculate_trend(data):
    result = {}
    for d in data:
        doi = d['doi']
        df = d['df']
        # print(doi)
        # print(len(df.index))
        trend = mk.yue_wang_modification_test(df)
        # print(trend.slope)
        result[doi] = trend.slope
    return result
    # print(results[0]['df'].head())


def get_dataframes(trending):
    p = {"_bucket": trending['name'],
         "_start": trending['duration'],
         }
    filter_obj = doi_filter_list(get_doi_list_trending(trending), p)
    query = """
    _stop = now()
    totalTable = from(bucket: _bucket)
        |> range(start: _start, stop: _stop)
        |> filter(fn: (r) => r["_measurement"] == "trending")
        |> filter(fn: (r) => r["_field"] == "score")"""
    query += filter_obj['string']
    query += """
        |> yield()
        """

    result = query_api.query(org=org, query=query, params=filter_obj['params'])
    results = []
    for table in result:
        scores = []
        times = []
        doi = None
        for record in table.records:
            if not doi:
                doi = record['doi']
            scores.append(record['_value'])
            times.append(record['_time'])
        df = pd.DataFrame(data={'score': scores}, index=times)
        results.append({"doi": doi, "df": df})

    return results


def get_base_trend_table(trending):
    p = {"_start": trending['duration'],
         "_bucket": trending['name'],
         "_exponent": trending['time_exponent'],
         "_window": trending['window_size'],
         "_window_count": trending['window_count'],
         }
    dois = get_doi_list_trending(trending)
    if len(dois) > 0:
        filter_obj = doi_filter_list(dois, p)
        query = '''
                    import "math"
                    import "experimental"
                    
                    _stop = now()                
                    baseTable = from(bucket: _bucket)
                        |> range(start: _start, stop: _stop)
                        |> filter(fn: (r) => r["_measurement"] == "trending") 
                '''
        query += filter_obj['string']
        query += '''                
        
                    windowTable = baseTable
                        |> filter(fn: (r) => r["_field"] == "score")
                        |> aggregateWindow(every: _window, fn: sum, createEmpty: true)
                        |> sort(columns: ["_time"], desc: true)
                        |> limit(n: _window_count)
                        |> sort(columns: ["_time"])
                        |> map(fn:(r) => ({
                            r with _value:
                            if exists r._value then r._value
                            else 0.0
                        }))
                        |> keep(columns: ["_time", "_value", "doi"])
                    
                    stddev = windowTable
                        |> stddev()
                        |> group()
                        |> keep(columns: ["_value", "doi"])
                        |> rename(columns: {_value: "stddev"})
                    
                    ema = windowTable
                        |> exponentialMovingAverage(n: _window_count)
                        |> group()
                        |> keep(columns: ["_value", "doi"])
                        |> rename(columns: {_value: "ema"})
                    
                    j1 = join(
                        tables: {stddev:stddev, ema:ema},
                        on: ["doi"]
                        )
                    
                    ker = windowTable
                        |> kaufmansER(n: _window_count - 1)
                        |> group()
                        |> keep(columns: ["_value", "doi"])
                        |> rename(columns: {_value: "ker"})
                    
                    j2 = join(
                        tables: {j1:j1, ker:ker},
                        on: ["doi"]
                        )
                    
                    kama = windowTable
                        |> kaufmansAMA(n: _window_count -1)
                        |> group()
                        |> keep(columns: ["_value", "doi"])
                        |> rename(columns: {_value: "kama"})
                    
                    j3 = join(
                        tables: {kama:kama, j2:j2},
                        on: ["doi"]
                        )
                    
                    mean = baseTable
                        |> filter(fn: (r) => r["_field"] == "score")
                        |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                        |> experimental.mean()
                        |> group()
                        |> keep(columns: ["_value", "doi"])
                        |> rename(columns: {_value: "mean"})
                    
                    j5 = join(
                        tables: {mean:mean, j3:j3},
                        on: ["doi"]
                        )
                    
                    prediction = windowTable
                        |> holtWinters(n: 1,  seasonality: 0, interval: _window, withFit: false, timeColumn: "_time", column: "_value")
                        |> group()
                        |> keep(columns: ["_value", "doi"])
                        |> rename(columns: {_value: "prediction"})
                    
                    j6 = join(
                        tables: {prediction:prediction, j5:j5},
                        on: ["doi"]
                        )
                    
                    score = baseTable
                      |> filter(fn: (r) => r["_field"] == "score")
                      |> keep(columns: ["_value", "_time", "doi"])
                      |> map(fn:(r) => ({ r with _value: float(v: r._value) * math.exp(x: _exponent * float(v: uint(v: now()) - uint(v: r._time)) / (10.0 ^ 9.0)) }))
                      |> cumulativeSum(columns: ["_value"])
                      |> last(column: "_value")
                      |> keep(columns: ["_value", "doi"])
                      |> group()
                      |> rename(columns: {_value: "score"})
                    
                    j7 = join(
                      tables: {j6:j6, score:score},
                      on: ["doi"]
                    )
                    
                    a = baseTable
                      |> filter(fn: (r) => r["_field"] == "followers")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> sum()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "sum_followers"})
                    
                    b = baseTable
                      |> filter(fn: (r) => r["_field"] == "sentiment_raw")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "mean_sentiment"})
                    
                    c = baseTable
                      |> filter(fn: (r) => r["_field"] == "contains_abstract_raw")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "contains_abstract_raw"})
                    
                    ad = baseTable
                      |> filter(fn: (r) => r["_field"] == "questions")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "questions"})
                    
                    e = baseTable
                      |> filter(fn: (r) => r["_field"] == "exclamations")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "exclamations"})
                    
                    f = baseTable
                      |> filter(fn: (r) => r["_field"] == "length")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "length"})
                    
                    g = baseTable
                      |> filter(fn: (r) => r["_field"] == "length")
                      |> map(fn: (r) => ({r with _value: math.round(x: float(v: uint(v: now()) - uint(v: r._time)) / (10.0 ^ 9.0)) }))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "mean_age"})
                    
                    j = baseTable
                      |> filter(fn: (r) => r["_field"] == "length")
                      |> count()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "count"})
                    
                    jk = baseTable
                      |> filter(fn: (r) => r["_field"] == "bot_rating")
                      |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                      |> experimental.mean()
                      |> group()
                      |> keep(columns: ["_value", "doi"])
                      |> rename(columns: {_value: "mean_bot_rating"})
                    
                    join1 = join(
                      tables: {a:a, b:b},
                      on: ["doi"]
                    )
                    
                    join2 = join(
                      tables: {join1:join1, c:c},
                      on: ["doi"]
                    )
                    
                    join3 = join(
                      tables: {join2:join2, ad:ad},
                      on: ["doi"]
                    )
                    
                    join4 = join(
                      tables: {join3:join3, e:e},
                      on: ["doi"]
                    )
                    
                    join46 = join(
                      tables: {join4:join4, j:j},
                      on: ["doi"]
                    )
                    
                    join5 = join(
                      tables: {join46:join46, f:f},
                      on: ["doi"]
                    )
                    
                    join6 = join(
                      tables: {join5:join5, g:g},
                      on: ["doi"]
                    )
                    
                    join68 = join(
                      tables: {join6:join6, jk:jk},
                      on: ["doi"]
                    )
                    
                    join9 = join(
                        tables: {join68:join68, j7:j7},
                        on: ["doi"]
                    )
                      |> sort(columns: ["score"], desc: true)
                      |> yield(name: "join10")
    
            '''
        a = time.time()
        tables = query_api.query(query, params=p)
        # print(query)

        # print('done pubs')
        session_factory = sessionmaker(bind=DAO.engine)
        Session = scoped_session(session_factory)
        session = Session()

        frames = get_dataframes(trending)
        trend = calculate_trend(frames)

        print(time.time() - a)
        # print('done trends ' + trending['name'])

        delete_trending_table(session, trending['name'])

        trending_objects = []
        for table in tables:
            for record in table.records:
                trending_value = 0
                if record['doi'] in trend:
                    trending_value = trend[record['doi']]

                save_trend_to_influx(record, trending_value, trending['trending_bucket'])

                t_obj = Trending(publication_doi=record['doi'],
                                 duration=trending['name'],
                                 score=record['score'], count=record['count'],
                                 mean_sentiment=record['mean_sentiment'],
                                 sum_followers=record['sum_followers'],
                                 mean_age=record['mean_age'],
                                 mean_length=record['length'],
                                 mean_questions=record['questions'],
                                 mean_exclamations=record['exclamations'],
                                 abstract_difference=record['contains_abstract_raw'],
                                 mean_bot_rating=record['mean_bot_rating'],
                                 ema=record['ema'],
                                 kama=record['kama'],
                                 ker=record['ker'],
                                 mean_score=record['mean'],
                                 stddev=record['stddev'],
                                 trending=trending_value,
                                 projected_change=record['prediction'])

                t_obj = save_or_update(session, t_obj, Trending,
                                       {'publication_doi': t_obj.publication_doi, 'duration': t_obj.duration})
                trending_objects.append(t_obj)
        return trending_objects
    return []


def delete_trending_table(session, duration):
    query = """
            DELETE FROM trending
                WHERE duration=:duration;
            """
    params = {'duration': duration, }
    s = text(query)
    s = s.bindparams(bindparam('duration'))
    return session.execute(s, params)


def save_or_update(session, obj, table, kwargs):
    obj_db = DAO.get_object(session, table, kwargs)
    if obj_db:
        session.delete(obj_db)
        session.commit()

    DAO.save_object(session, obj)
    return obj


def save_trend_to_influx(record, trending_value, bucket):
    point = {
        "measurement": "trending",
        "tags": {
            "doi": record['doi']
        },
        "fields": {
            "score": record['score'],
            "count": record['count'],
            "mean_sentiment": record['mean_sentiment'],
            "sum_follower": record['sum_followers'],
            "mean_age": record['mean_age'],
            "mean_length": record['length'],
            "mean_questions": record['questions'],
            "mean_exclamations": record['exclamations'],
            "abstract_difference": record['contains_abstract_raw'],
            "mean_bot_rating": record['mean_bot_rating'],
            "ema": record['ema'],
            "kama": record['kama'],
            "ker": record['ker'],
            "mean_score": record['mean'],
            "stddev": record['stddev'],
            "trending": trending_value,
            "projected_change": record['prediction']
        },
        "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}

    # print(bucket)
    # print(point)
    write_api.write(bucket, org, [point])


async def write_event(data):
    loop = asyncio.get_event_loop()
    # Using None will create a ThreadPoolExecutor
    # you can also pass in an executor (Thread or Process)
    await loop.run_in_executor(None, save_data_to_influx, data)


def save_data_to_influx(data):
    doi = data['obj']['data']['doi']
    createdAt = data['timestamp']
    score = data['subj']['processed']['score']

    point = {
        "measurement": "trending",
        "tags": {
            "doi": doi
        },
        "fields": {
            "score": float(score),
            "contains_abstract_raw": float(
                data['subj']['processed']['contains_abstract_raw']) if 'contains_abstract_raw' in data['subj'][
                'processed'] else 0.0,
            "sentiment_raw": float(data['subj']['processed']['sentiment_raw']),
            "followers": data['subj']['processed']['followers'],
            "length": data['subj']['processed']['length'],
            "questions": data['subj']['processed']['question_mark_count'],
            "exclamations": data['subj']['processed']['exclamation_mark_count'],
            "bot_rating": data['subj']['processed']['bot_rating'],
            "time_score": float(data['subj']['processed']['time_score']),
            "type_factor": float(data['subj']['processed']['type_factor']),
            "user_score": float(data['subj']['processed']['user_score']),
            "content_score": float(data['subj']['processed']['content_score']),
            "count": 1
        },
        "time": createdAt}

    write_api.write('currently', org, [point])


def doi_filter_list(doi_list, params):
    filter_string = "|> filter(fn: (r) =>"
    i = 0
    for doi in doi_list:
        filter_string += 'r["doi"] == _doi_nr_' + str(i) + ' or '
        params['_doi_nr_' + str(i)] = doi
        i += 1
    filter_string = filter_string[:-4] + ')'
    return {"string": filter_string, "params": params}


# sink=[aggregated_topic]
@app.agent(processed_topic)
async def aggregate(events):
    """ aggregate events

    Arguments:
        events: events from topic
    """
    async for event in events:
        await write_event(event)


if __name__ == '__main__':
    app.main()
