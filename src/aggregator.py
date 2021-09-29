import copy
import datetime
import os
import time
import types
import uuid
from datetime import timedelta
from heapq import heappop
from typing import cast
from influxdb_client.client.write_api import ASYNCHRONOUS
import asyncio
from event_stream import dao
from event_stream.models.model import *
from influxdb_client import InfluxDBClient, BucketRetentionRules
from sqlalchemy.orm import sessionmaker, scoped_session
import pandas as pd
import pymannkendall as mk
from sqlalchemy import text, bindparam

import faust
from faust.types.windows import WindowT

broker = os.environ.get('KAFKA_BOOTRSTRAP_SERVER', 'kafka:9092')
app = faust.App(
    'aggregator',
    broker=broker,
    stream_publish_on_commit=False,
    producer_max_request_size=1500000,
    topic_partitions=1,
)

DAO = dao.DAO()

processed_topic = app.topic('events_processed-discusses')
aggregated_topic = app.topic('events_aggregated')

org = os.environ.get('INFLUXDB_V2_ORG', 'ambalytics')

client = InfluxDBClient.from_env_properties()
write_api = client.write_api(write_options=ASYNCHRONOUS)
query_api = client.query_api()

bucket_definition = [
    {
        'name': 'trending',
        'retention': timedelta(days=30),
        'downsample_bucket': None,
    }, {
        'name': 'history',
        'retention': None,
        'downsample_bucket': timedelta(hours=1),
    },
]

trending_time_definition = {
    'now': {
        'name': 'now',
        'bucket': 'trending',
        'duration': timedelta(hours=-6),
        'trending_interval': timedelta(minutes=3),  # 100 times in interval? duration(min) / 100
        'time_exponent': -0.00036,  # base value for one hour -> -0.0006 * duration(hours)
        'window_size': timedelta(minutes=6),  # 60? so we don't need window count?
        'window_count': 60,  # from window size or "fixed"
        'min_count': 20,  #  n per hour?
    },
    'today': {
        'name': 'today',
        'bucket': 'trending',
        'duration': timedelta(hours=-24),

        'trending_interval': timedelta(minutes=60),
        'time_exponent': -0.000025,
        'window_size': timedelta(minutes=24),
        'min_count': 80,
        'window_count': 60,
    },
    'week': {
        'name': 'week',
        'bucket': 'trending',
        'duration': timedelta(days=-7),
        'trending_interval': timedelta(hours=6),
        'time_exponent': -0.00000357142857,
        'window_size': timedelta(minutes=168),
        'min_count': 200,
        'window_count': 60,
    },
    'month': {
        'name': 'month',
        'bucket': 'trending',
        'duration': timedelta(days=-30),
        'trending_interval': timedelta(hours=12),
        'time_exponent': -0.000000833333333,
        'window_size': timedelta(minutes=720),
        'min_count': 500,
        'window_count': 60,
    },
    'year': {
        'name': 'month',
        'bucket': 'trending',
        'duration': timedelta(days=-30),
        'trending_interval': timedelta(hours=12),
        'time_exponent': -0.000000833333333,
        'window_size': timedelta(minutes=720),
        'min_count': 500,
        'window_count': 60,
    },
    'history': {
        'name': 'month',
        'bucket': 'trending',
        'duration': timedelta(days=-30),
        'trending_interval': timedelta(hours=12),
        'time_exponent': -0.000000833333333,
        'window_size': timedelta(minutes=720),
        'min_count': 500,
        'window_count': 60,
    }
}


@app.task
async def init_influx():
    buckets_api = client.buckets_api()
    buckets = buckets_api.find_buckets().buckets
    for td in bucket_definition:
        exist = False
        for b in buckets:
            if b.name == td['name']:
                exist = True
        if not exist:
            if td['retention']:
                retention_rules = BucketRetentionRules(type="expire",
                                                       every_seconds=int(td['retention'].total_seconds()))
                created_bucket = buckets_api.create_bucket(bucket_name=td['name'],
                                                           retention_rules=retention_rules, org=org)
            else:
                created_bucket = buckets_api.create_bucket(bucket_name=td['name'], org=org)

    # https://influxdb-client.readthedocs.io/en/stable/api.html#tasksapi

    org_api = client.organizations_api()
    org_obj = org_api.find_organizations(org=org)[0]

    tasks_api = client.tasks_api()
    task = """
        _window = 1h
        
        baseTable = from(bucket: "trending")
          |> range(start: -6h, stop: now())
          |> filter(fn: (r) => r["_measurement"] == "trending")
        
        a = baseTable
          |> filter(fn: (r) => r["_field"] == "followers")
          |> aggregateWindow(fn: sum, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> rename(columns: {_value: "sum_followers"})
        
        b = baseTable
          |> filter(fn: (r) => r["_field"] == "sentiment_raw")
          |> aggregateWindow(fn: median, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> rename(columns: {_value: "median_sentiment"})
        
        join1 = join(
          tables: {a:a, b:b},
          on: ["doi", "_time"]
        )
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
        
        c = baseTable
          |> filter(fn: (r) => r["_field"] == "contains_abstract_raw")
          |> aggregateWindow(fn: median, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
          |> rename(columns: {_value: "contains_abstract_raw"})
        
        join2 = join(
          tables: {join1:join1, c:c},
          on: ["doi", "_time"]
        )
        
        ad = baseTable
          |> filter(fn: (r) => r["_field"] == "questions")
          |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
          |> rename(columns: {_value: "questions"})
        
        join3 = join(
          tables: {join2:join2, ad:ad},
          on: ["doi", "_time"]
        )
        
        e = baseTable
          |> filter(fn: (r) => r["_field"] == "exclamations")
          |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
          |> rename(columns: {_value: "exclamations"})
        
        join4 = join(
          tables: {join3:join3, e:e},
          on: ["doi", "_time"]
        )
        
        j = baseTable
          |> filter(fn: (r) => r["_field"] == "length")
          |> aggregateWindow(fn: count, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
          |> rename(columns: {_value: "count"})
        
        join46 = join(
          tables: {join4:join4, j:j},
          on: ["doi", "_time"]
        )
        
        f = baseTable
          |> filter(fn: (r) => r["_field"] == "length")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> aggregateWindow(fn: median, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
          |> rename(columns: {_value: "length"})
        
        join5 = join(
          tables: {join46:join46, f:f},
          on: ["doi", "_time"]
        )
        
        jk = baseTable
          |> filter(fn: (r) => r["_field"] == "bot_rating")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> aggregateWindow(fn: mean, every: _window, createEmpty: false)
          |> group()
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn: (r) => ({r with _time: uint(v: r._time)}))
          |> rename(columns: {_value: "mean_bot_rating"})
        
        join6 = join(
          tables: {join5:join5, jk:jk},
          on: ["doi", "_time"]
        )
          |> map(fn:(r) => ({ r with _time: time(v:r._time) }))
          |> group(columns: ["doi"])
          |> to(bucket: "history", org: "ambalytics")
    """
    task = tasks_api.create_task_every(name="make_history", flux=task, every="6h", organization=org_obj)


async def run_trend_calculation(trending_time):
    loop = asyncio.get_event_loop()
    # Using None will create a ThreadPoolExecutor
    # you can also pass in an executor (Thread or Process)
    await loop.run_in_executor(None, get_base_trend_table, trending_time)


@app.timer(interval=trending_time_definition['now']['trending_interval'])
async def trend_calc():
    print('calc trend hour')
    await run_trend_calculation(trending_time_definition['now'])


@app.timer(interval=trending_time_definition['today']['trending_interval'])
async def trend_calc():
    print('calc trend today')
    await run_trend_calculation(trending_time_definition['today'])


# @app.timer(interval=trending_time_definition['week']['trending_interval'])
# async def trend_calc():
#     print('calc trend week')
#     get_base_trend_table(trending_time_definition['week'])


def get_doi_list_trending(trending):
    p = {"_bucket": trending['bucket'],
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
    print(results)
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
    p = {"_bucket": trending['bucket'],
         "_doi_list": get_doi_list_trending(trending),
         "_start": trending['duration'],
         }
    query = """
    _stop = now()
    totalTable = from(bucket: "trending")
        |> range(start: _start, stop: _stop)
        |> filter(fn: (r) => r["_measurement"] == "trending")
        |> filter(fn: (r) => r["_field"] == "score")
        |> filter(fn: (r) => contains(value: r["doi"], set: _doi_list))
        |> yield()
    """
    # data = query_api.query_data_frame(org=org, query=query, params=p)

    result = query_api.query(org=org, query=query, params=p)
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
         "_bucket": trending['bucket'],
         "_exponent": trending['time_exponent'],
         "_window": trending['window_size'],
         "_window_count": trending['window_count'],
         "_doi_list": get_doi_list_trending(trending)
         }
    query = '''
                import "math"
                import "experimental"
                
                _stop = now()                
                baseTable = from(bucket: _bucket)
                    |> range(start: _start, stop: _stop)
                    |> filter(fn: (r) => r["_measurement"] == "trending")
                    |> filter(fn: (r) => contains(value: r["doi"], set: _doi_list))
                
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
                
                mean = windowTable
                    |> mean()
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
                  |> median()
                  |> group()
                  |> keep(columns: ["_value", "doi"])
                  |> rename(columns: {_value: "median_sentiment"})
                
                c = baseTable
                  |> filter(fn: (r) => r["_field"] == "contains_abstract_raw")
                  |> map(fn: (r) => ({r with _value: float(v: r._value)}))
                  |> median()
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
                  |> median()
                  |> group()
                  |> keep(columns: ["_value", "doi"])
                  |> rename(columns: {_value: "length"})
                
                g = baseTable
                  |> filter(fn: (r) => r["_field"] == "length")
                  |> map(fn: (r) => ({r with _value: math.round(x: float(v: uint(v: now()) - uint(v: r._time)) / (10.0 ^ 9.0)) }))
                  |> median()
                  |> group()
                  |> keep(columns: ["_value", "doi"])
                  |> rename(columns: {_value: "median_age"})
                
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
    print(time.time() - a)

    print('done pubs')
    session_factory = sessionmaker(bind=DAO.engine)
    Session = scoped_session(session_factory)
    session = Session()

    frames = get_dataframes(trending)
    trend = calculate_trend(frames)

    print('done trends')

    delete_trending_table(session, abs(trending['duration'].total_seconds()))

    trending_objects = []
    for table in tables:
        for record in table.records:
            t_obj = Trending(publication_doi=record['doi'], duration=abs(trending['duration'].total_seconds()),
                             score=record['score'], count=record['count'],
                             median_sentiment=record['median_sentiment'],
                             sum_follower=record['sum_followers'],
                             median_age=record['median_age'],
                             median_length=record['length'],
                             mean_questions=record['questions'],
                             mean_exclamations=record['exclamations'],
                             abstract_difference=record['contains_abstract_raw'],
                             mean_bot_rating=record['mean_bot_rating'],
                             ema=record['ema'],
                             kama=record['kama'],
                             ker=record['ker'],
                             mean_score=record['mean'],
                             stddev=record['stddev'],
                             trending=trend[record['doi']],
                             projected_change=record['prediction'])

            t_obj = save_or_update(session, t_obj, Trending,
                                {'publication_doi': t_obj.publication_doi, 'duration': t_obj.duration})
            trending_objects.append(t_obj)
    return trending_objects


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


def save_data_to_influx(data):
    doi = data['obj']['data']['doi']
    createdAt = data['timestamp']
    score = data['subj']['processed']['score']
    # todo save id or index instead of the actual string since its only for diversity?

    point = {
        "measurement": "trending",
        "tags": {
            "doi": doi
        },
        "fields": {
            "score": score,
            "contains_abstract_raw": float(
                data['subj']['processed']['contains_abstract_raw']) if 'contains_abstract_raw' in data['subj'][
                'processed'] else 0.0,
            "sentiment_raw": float(data['subj']['processed']['sentiment_raw']),
            "followers": data['subj']['processed']['followers'],
            "length": data['subj']['processed']['length'],
            "questions": data['subj']['processed']['question_mark_count'],
            "exclamations": data['subj']['processed']['exclamation_mark_count'],
            "bot_rating": data['subj']['processed']['bot_rating'],
        },
        "time": createdAt}

    print(point)
    write_api.write('trending', org, [point])


# sink=[aggregated_topic]
@app.agent(processed_topic)
async def aggregate(events):
    """ aggregate events

    Arguments:
        events: events from topic
    """
    async for event in events:
        save_data_to_influx(event)


if __name__ == '__main__':
    app.main()
