import copy
import datetime
import os
import time
import types
import uuid
from datetime import timedelta
from heapq import heappop
from typing import cast

from event_stream import dao
from event_stream.models.model import *
from influxdb_client import InfluxDBClient, BucketRetentionRules
from sqlalchemy.orm import sessionmaker, scoped_session

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
write_api = client.write_api()
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
    'hour': {
        'name': 'hour',
        'bucket': 'trending',
        'duration': timedelta(hours=-1),
        'trending_interval': timedelta(minutes=1),
        'time_exponent': -0.0006,
        'window_size': timedelta(minutes=6),
    },
    'today': {
        'name': 'today',
        'bucket': 'trending',
        'duration': timedelta(hours=-24),
        'trending_interval': timedelta(minutes=5),
        'time_exponent': -0.000025,
        'window_size': timedelta(minutes=144),
    },
    'week': {
        'name': 'week',
        'bucket': 'trending',
        'duration': timedelta(days=-7),
        'trending_interval': timedelta(hours=1),
        'time_exponent': -0.00000357142857,
        'window_size': timedelta(minutes=1008),
    },
    'month': {
        'name': 'month',
        'bucket': 'trending',
        'duration': timedelta(days=-30),
        'trending_interval': timedelta(hours=6),
        'time_exponent': -0.000000833333333,
        'window_size': timedelta(minutes=4320),
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
            # todo setup tasks
            # https://influxdb-client.readthedocs.io/en/stable/api.html#tasksapi


@app.timer(interval=trending_time_definition['hour']['trending_interval'])
async def trend_calc():
    print('calc trend hour')
    # bucket = trending_time_definition['hour']['bucket']
    get_base_trend_table(trending_time_definition['hour'])
    # get all scores

    # create dict doi => trending object
    # update them on doing further stuff

    # get count
    # get median sentiment

    # sum_follower

    # we wan't rows to save into postgres

    # select all rows with this duration
    # delete them
    # than save new
    # print(results)


@app.timer(interval=trending_time_definition['today']['trending_interval'])
async def trend_calc():
    print('calc trend today')
    get_base_trend_table(trending_time_definition['today'])


@app.timer(interval=trending_time_definition['week']['trending_interval'])
async def trend_calc():
    print('calc trend week')
    get_base_trend_table(trending_time_definition['week'])


# import "math"
# from(bucket: "discussion")
#   |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
#   |> filter(fn: (r) => r["_measurement"] == "trending")
#   |> filter(fn: (r) => r["_field"] == "score")
#   //|> aggregateWindow(every: 5s, fn: sum, createEmpty: false)
#   //|> map(fn: (r) => ({ time: uint(v: now()) - uint(v: r._time) }))
#   //|> map(fn: (r) => ({ time: 0.1*float(v: uint(v: now()) - uint(v: r._time)) }))
#   //|> map(fn: (r) => ({ _value: float(v: r._value) * 22.1 }))
#   //|> map(fn:(r) => ({ r with _value: float(v: r._value) * 22.2 }))
#   |> map(fn:(r) => ({ r with _value: float(v: r._value) * math.exp(x: -0.000000000001*float(v: uint(v: now()) - uint(v: r._time))) }))
#   //|> map(fn:(r) => ({ r with _value:  float(v: uint(v: now()) - uint(v: r._time)) * 0.000000004 }))
#   |> cumulativeSum(columns: ["_value"])


def get_base_query(start=60, stop=None, doi=None):
    query = '''
        import "math"
        from(bucket: _bucket)
        |> range(start: _start, stop: _stop)
        |> filter(fn: (r) => r["_measurement"] == "trending")
        |> filter(fn: (r) => r["_field"] == "score")
        |> group(columns: ["doi"])
        '''
    if doi:
        query += '|> filter(fn: (r) => r["doi"] == _doi)'
    return query


def run_query(query, params):
    result = query_api.query(org=org, query=query, params=params)
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_value(), record.get_field()))
    print(results)
    return results


#
# def get_time_adjusted_score_sum(trending_time_name, doi=None):
#     query = get_base_query(time_interval, doi)
#
#     query += '''
#           |> map(fn:(r) => ({ r with _value: float(v: r._value) * math.exp(x: _exp*float(v: uint(v: now()) - uint(v: r._time))) }))
#           |> cumulativeSum(columns: ["_value"])
#           |> last()
#
#
#     calculate_trending_score = """
#         import "math"
#         from(bucket: "trending")
#           |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
#           |> filter(fn: (r) => r["_measurement"] == "trending")
#           |> filter(fn: (r) => r["_field"] == "score")
#           |> keep(columns: ["_value", "_time", "doi"])
#           |> map(fn:(r) => ({ r with _value: float(v: r._value) * math.exp(x: -0.0000000000006*float(v: uint(v: now()) - uint(v: r._time))) }))
#           |> cumulativeSum(columns: ["_value"])
#           |> last(column: "_value")
#           |> keep(columns: ["_value", "doi"])
#           |> group(columns: ["_value", "doi"])
#           |> sort(columns: ["_value"], desc: true)
#     """
#     '''
#     return run_query(query)


def get_base_trend_table(trending):
    p = {"_start": trending['duration'],
         "_bucket": trending['bucket'],
         "_exponent": trending['time_exponent'],
         "_window": trending['window_size'],
         }
    query = '''
            import "math"
            a = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "followers")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> sum()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "sum_followers"})
            
            b = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "sentiment_raw")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> median()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "median_sentiment"})
              
            c = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "contains_abstract_raw")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> median()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "contains_abstract_raw"})
            
            ad = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "questions")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> mean()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "questions"})
            
            e = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "exclamations")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> mean()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "exclamations"})
            
            f = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "length")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> median()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "length"})
            
            g = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "length")
              |> map(fn: (r) => ({r with _value: math.round(x: float(v: uint(v: now()) - uint(v: r._time)) / (10.0 ^ 9.0)) }))
              |> median()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "median_age"})
            
            j =  from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> count()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "count"})
            
            score = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "score")
              |> keep(columns: ["_value", "_time", "doi"])
              |> map(fn:(r) => ({ r with _value: float(v: r._value) * math.exp(x: _exponent * float(v: uint(v: now()) - uint(v: r._time)) / (10.0 ^ 9.0)) }))
              |> cumulativeSum(columns: ["_value"])
              |> last(column: "_value")
              |> keep(columns: ["_value", "doi"])
              |> group()
              |> rename(columns: {_value: "score"})
            
            predict = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "score")
              |> aggregateWindow(every: _window, fn: sum, createEmpty: true)
              |> holtWinters(n: 1,  seasonality: 0, interval: _window, withFit: false, timeColumn: "_time", column: "_value")
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "prediction"})
            
            
            pcompare = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "score")
              |> aggregateWindow(every: _window, fn: sum, createEmpty: true)
              |> last()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "last_window"})
            
            pmedian = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "score")
              |> aggregateWindow(every: _window, fn: sum, createEmpty: true)
              |> median()
              |> group()
              |> keep(columns: ["_value", "doi"])
              |> rename(columns: {_value: "window_median"})
            
            // pcompare |> yield(name: "pcompare")
            
            join0 = join(
              tables: {score:score, a:a},
              on: ["doi"])
            
            join1 = join(
              tables: {join0:join0, b:b},
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
            
            join7 = join(
              tables: {join6:join6, predict:predict},
              on: ["doi"]
            )
            
            join8 = join(
              tables: {join7:join7, pcompare:pcompare},
              on: ["doi"]
            )
            
            join9 = join(
              tables: {join8:join8, pmedian:pmedian},
              on: ["doi"]
            )
        
            aaaa = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> group(columns: ["doi", "language"])
              |> distinct()
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "species"})
            
            ffff = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> group(columns: ["doi"])
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "total"})
            
            cccc = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> duplicate(column: "_value", as: "language")
              |> group(columns: ["doi", "language"])
              |> count()
              |> keep(columns: ["doi", "language", "_value"])
              |> group()
              |> rename(columns: {_value: "n"})
            
            gggg = join(
              tables: {ffff:ffff, cccc:cccc},
              on: ["doi"]
            )
              |> map(fn: (r) => ({ r with _value: -1.0 * float(v: r.n) / float(v: r.total)  * math.log(x: float(v: r.n) / float(v: r.total)) }))
              |> group(columns: ["doi"])
              |> sum()
              |> group()
              |> rename(columns: {_value: "shanon"})
            
            eeee = join(
              tables: {aaaa:aaaa, gggg:gggg},
              on: ["doi"]
            )
            |> map(fn: (r) => ({ r with eveness_lang: 
                if r.species > 1 and r.shanon > 0 then float(v: r.shanon) / math.log(x: float(v: r.species)) 
                else 1.0
              }))
            |> keep(columns: ["doi", "eveness_lang"])
        
        
            aa = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_name")
              |> group(columns: ["doi", "author_name"])
              |> distinct()
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "species"})
            
            ff = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_name")
              |> group(columns: ["doi"])
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "total"})
            
            cc = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_name")
              |> duplicate(column: "_value", as: "author_name")
              |> group(columns: ["doi", "author_name"])
              |> count()
              |> keep(columns: ["doi", "author_name", "_value"])
              |> group()
              |> rename(columns: {_value: "n"})
            
            gg = join(
              tables: {ff:ff, cc:cc},
              on: ["doi"]
            )
              |> map(fn: (r) => ({ r with _value: -1.0 * float(v: r.n) / float(v: r.total)  * math.log(x: float(v: r.n) / float(v: r.total)) }))
              |> group(columns: ["doi"])
              |> sum()
              |> group()
              |> rename(columns: {_value: "shanon"})
            
            ee = join(
              tables: {aa:aa, gg:gg},
              on: ["doi"]
            )
            |> map(fn: (r) => ({ r with eveness_author: 
                if r.species > 1 and r.shanon > 0 then float(v: r.shanon) / math.log(x: float(v: r.species)) 
                else 1.0
              }))
            |> keep(columns: ["doi", "eveness_author"])
        
            aaa = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_location")
              |> group(columns: ["doi", "author_location"])
              |> distinct()
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "species"})
            
            fff = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_location")
              |> group(columns: ["doi"])
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "total"})
            
            ccc = from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_location")
              |> duplicate(column: "_value", as: "author_location")
              |> group(columns: ["doi", "author_location"])
              |> count()
              |> keep(columns: ["doi", "author_location", "_value"])
              |> group()
              |> rename(columns: {_value: "n"})
            
            ggg = join(
              tables: {fff:fff, ccc:ccc},
              on: ["doi"]
            )
              |> map(fn: (r) => ({ r with _value: -1.0 * float(v: r.n) / float(v: r.total)  * math.log(x: float(v: r.n) / float(v: r.total)) }))
              |> group(columns: ["doi"])
              |> sum()
              |> group()
              |> rename(columns: {_value: "shanon"})
            
            eee = join(
              tables: {aaa:aaa, ggg:ggg},
              on: ["doi"]
            )
            |> map(fn: (r) => ({ r with eveness_location: 
                if r.species > 1 and r.shanon > 0 then float(v: r.shanon) / math.log(x: float(v: r.species)) 
                else 1.0
              }))
            |> keep(columns: ["doi", "eveness_location"])
        
            nn = join(
              tables: {eeee:eeee, ee:ee},
              on: ["doi"]
            )
            nnn =  join(
              tables: {nn:nn, eee:eee},
              on: ["doi"]
            )a
            
            join10 = join(
              tables: {join9:join9, nnn:nnn},
              on: ["doi"]
            )
            
            join10
              |> sort(columns: ["score"], desc: true)
              |> yield(name: "join10")
        '''
    tables = query_api.query(query, params=p)

    session_factory = sessionmaker(bind=DAO.engine)
    Session = scoped_session(session_factory)
    session = Session()

    # trending_objects = []
    for table in tables:
        # print(table)
        for record in table.records:
            pc = record['prediction'] / record['window_median']
            t_obj = Trending(publication_doi=record['doi'], duration=abs(trending['duration'].total_seconds()),
                             score=record['score'], count=record['count'],
                             median_sentiment=record['median_sentiment'],
                             sum_follower=record['sum_followers'],
                             median_age=record['median_age'],
                             median_length=record['length'],
                             avg_questions=record['questions'],
                             avg_exclamations=record['exclamations'],
                             abstract_difference=record['contains_abstract_raw'],
                             location_diversity=record['eveness_location'],
                             tweet_author_diversity=record['eveness_author'],
                             lan_diversity=record['eveness_lang'],
                             projected_change=pc)
            t_obj = save_or_update(session, t_obj, Trending,
                                   {'publication_doi': t_obj.publication_doi, 'duration': t_obj.duration})
            # trending_objects.append(t_obj)


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
            "time_score": float(data['subj']['processed']['time_score']),
            "type_score": data['subj']['processed']['type_score'],
            "lang": data['subj']['data']['lang'],
            "contains_abstract_raw": float(
                data['subj']['processed']['contains_abstract_raw']) if 'contains_abstract_raw' in data['subj'][
                'processed'] else 0.0,
            "contains_abstract": data['subj']['processed']['contains_abstract'],
            "sentiment_raw": float(data['subj']['processed']['sentiment_raw']),
            "sentiment": data['subj']['processed']['sentiment'],
            "followers": data['subj']['processed']['followers'],
            "type": data['subj']['processed']['tweet_type'],
            "length": data['subj']['processed']['length'],
            "questions": data['subj']['processed']['question_mark_count'],
            "exclamations": data['subj']['processed']['exclamation_mark_count'],
            "bot_rating": data['subj']['processed']['bot_rating'],
            "author_name": data['subj']['processed']['name'],
            "author_location": data['subj']['processed']['location'],
        },
        "time": createdAt}

    # print(point)
    write_api.write('trending', org, [point])

    downsample = """
    
            // Task Options
            option task = {
              name: "trending-1w",
              every: 1w,
            }
            
            // Defines a data source
            data = from(bucket: "system-data")
              |> range(start: -duration(v: int(v: task.every) * 2))
              |> filter(fn: (r) => r._measurement == "mem")
            
            data
              // Windows and aggregates the data in to 1h averages
              |> aggregateWindow(fn: mean, every: 1h)
              // Stores the aggregated data in a new bucket
              |> to(bucket: "system-data-downsampled", org: "my-org")

    
                from(bucket: "trending")
              |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
              |> filter(fn: (r) => r["doi"] == "10.1136/bmj.n2211")
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> duplicate(column: "_value", as: "language")
              |> group(columns: ["language"])
              |> count()
              
              
    """

    # sentiment_raw, score, contains_abstract
    average_score = """
        median = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> keep(columns: ["_value"])
          |> group()
          |> median()
          |> set(key: "name", value: "median")
          |> group(columns: ["name"])
        
        min = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> keep(columns: ["_value"])
          |> group()
          |> min()
          |> set(key: "name", value: "min")
          |> group(columns: ["name"])
        
        max = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")  
          |> keep(columns: ["_value"])
          |> group()
          |> max()
          |> set(key: "name", value: "max")
          |> group(columns: ["name"])
        
        union(tables: [median, min, max])
        
        # followers(log result?), length
        median = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "length")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> median()
          |> set(key: "name", value: "median")
          |> group(columns: ["name"])
        
        min = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "length")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> min()
          |> set(key: "name", value: "min")
          |> group(columns: ["name"])
        
        max = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "length")  
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> max()
          |> set(key: "name", value: "max")
          |> group(columns: ["name"])
        
        
        union(tables: [median, min, max])
        
        # exclamations, questions
         mean = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "questions")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> mean()
          |> set(key: "name", value: "mean")
          |> group(columns: ["name"])
        
        min = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "questions")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> min()
          |> set(key: "name", value: "min")
          |> group(columns: ["name"])
        
        max = from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "questions")  
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> max()
          |> set(key: "name", value: "max")
          |> group(columns: ["name"])
        
        
        union(tables: [mean, min, max])
    """


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

    something = """
    // author_location, author_name
     from(bucket: "trending")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) => r["_measurement"] == "trending")
      |> filter(fn: (r) => r["_field"] == "lang")
      |> duplicate(column: "_value", as: "language")
      |> group(columns: ["language"])
      |> count()
    
    
     a = from(bucket: "trending")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) => r["_measurement"] == "trending")
      |> filter(fn: (r) => r["_field"] == "lang")
      |> keep(columns: ["_value"])
      |> group()
      |> count()
      |> set(key: "language", value: "total")
    
      //|> group(columns: ["language"])
    b = from(bucket: "trending")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) => r["_measurement"] == "trending")
      |> filter(fn: (r) => r["_field"] == "lang")
      |> duplicate(column: "_value", as: "language")
      |> group(columns: ["language"])
      |> count()
    
    union(tables: [a,b])
    """

    # last is about 11%
    exp = {
        "hour": -0.0006,
        "day": -0.000025,
        "week": -0.00000357142857,
        "month": -0.000000833333333,
    }

    calculate_trending_score = """
        import "math"
        from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> keep(columns: ["_value", "_time", "doi"])
          |> map(fn:(r) => ({ r with _value: float(v: r._value) * math.exp(x: -0.0000000000006*float(v: uint(v: now()) - uint(v: r._time))) }))
          |> cumulativeSum(columns: ["_value"])
          |> last(column: "_value")
          |> keep(columns: ["_value", "doi"])
          |> group(columns: ["_value", "doi"])
          |> sort(columns: ["_value"], desc: true)
    """

    window_size = {
        "hour": timedelta(minutes=6),
        "day": timedelta(minutes=144),
        "week": timedelta(minutes=1008),
        "month": timedelta(minutes=4320),
    }
    # slow
    predict_score = """
        import "math"
        from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> aggregateWindow(every: 6m, fn: sum, createEmpty: true)
          |> cumulativeSum(columns: ["_value"])
          |> holtWinters(n: 1,  seasonality: 0, interval: 6m, withFit: false, timeColumn: "_time", column: "_value")
          |> group()
          |> sort(desc: true)
    """

    median_sentiment = """
        from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "sentiment_raw")
          |> filter(fn: (r) => r["doi"] == "10.1001/jama.2021.1031")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> keep(columns: ["_value"])
          |> group()
          |> median()
    """

    ## wrong place
    median_follower_sum = """
        from(bucket: "trending")
          |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "followers")
          |> map(fn: (r) => ({r with _value: float(v: r._value)}))
          |> sum()
          |> group()
          |> median()
      """

    median_follower_sum = """
            from(bucket: "trending")
              |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "followers")
              |> map(fn: (r) => ({r with _value: float(v: r._value)}))
              |> sum()
              |> median()
          """

    shanon_eveness = """
        import "math"
            a = from(bucket: "trending")
              |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> group(columns: ["doi", "language"])
              |> distinct()
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "species"})
            
            f = from(bucket: "trending")
              |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> group(columns: ["doi"])
              |> count()
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> rename(columns: {_value: "total"})
            
            c = from(bucket: "trending")
              |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> duplicate(column: "_value", as: "language")
              |> group(columns: ["doi", "language"])
              |> count()
              |> keep(columns: ["doi", "language", "_value"])
              |> group()
              |> rename(columns: {_value: "n"})
            
            g = join(
              tables: {f:f, c:c},
              on: ["doi"]
            )
              |> map(fn: (r) => ({ r with _value: -1.0 * float(v: r.n) / float(v: r.total)  * math.log(x: float(v: r.n) / float(v: r.total)) }))
              |> group(columns: ["doi"])
              |> sum()
              |> group()
              |> rename(columns: {_value: "shanon"})
            
            e = join(
              tables: {a:a, g:g},
              on: ["doi"]
            )
            
            e |> map(fn: (r) => ({ r with eveness: 
                if r.species > 1 then float(v: r.shanon) / math.log(x: float(v: r.species)) 
                else 1.0
              }))
              |> keep(columns: ["doi", "eveness"])
              |> yield(name: "e")
      """
