import copy
import datetime
import os
import time
import types
import uuid
from datetime import timedelta
from heapq import heappop
from typing import cast

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


# todo wait for kafka

class Processed(faust.Record):
    score: float


class Subj(faust.Record):
    processed: Processed


class Event(faust.Record):
    id: str
    obj_id: str
    subj: Subj

    def get_score(self):
        """ get score of this event """
        return self.subj.processed.score

    def __lt__(self, other):
        """ we want an element to be sorted by score """
        return self.get_score() < other.get_score()


def get_json(data):
    result = {'id': str(uuid.uuid4()), 'relation_type': 'discusses', 'state': 'aggregated',
              'created_at': '{0:%Y-%m-%dT%H:%M:%SZ}'.format(datetime.datetime.now())}
    aggregated_data = {}
    i = 0
    for item in data:
        aggregated_data[i] = {
            'doi': item[0],
            'score': item[1]
        }
        i += 1
    result['aggregated_data'] = aggregated_data
    # print(len(result))
    return result


def _custom_del_old_keys(self) -> None:
    window = cast(WindowT, self.window)
    assert window
    for partition, timestamps in self._partition_timestamps.items():
        while timestamps and window.stale(
                timestamps[0],
                self._partition_latest_timestamp[partition]):
            timestamp = heappop(timestamps)
            keysList = [self._partition_timestamp_keys.get((partition, window_range[1])) for window_range in
                        self._window_ranges(timestamp)]
            keys_to_remove = self._partition_timestamp_keys.pop(
                (partition, timestamp), None)
            if keys_to_remove:
                windowData = [item for keys in keysList for key in keys for item in self.data.get(key, None)]
                for key in keys_to_remove:
                    value = self.data.pop(key, None)
                    if key[1][0] > self.last_closed_window:
                        self.on_window_close(key, windowData)
                self.last_closed_window = max(
                    self.last_closed_window,
                    max(key[1][0] for key in keys_to_remove),
                )


# def window_processor(key, event):
# print(key)
# print(event)
# print('window end')


processed_topic = app.topic('events_processed-discusses', value_type=Event)
aggregated_topic = app.topic('events_aggregated')
# count_table = app.Table('count_processed_events', default=int).hopping(10, 5, expires=timedelta(minutes=10))
# time in seconds, first is window size, second is time between creation
time_main = 6

score_table = app.Table('score_processed_events', default=int, partitions=1) \
    .hopping(timedelta(minutes=60 * time_main), timedelta(minutes=5), expires=timedelta(minutes=60 * time_main + 5), key_index=True)


# on_window_close=window_processor

# score_table._del_old_keys = types.MethodType(_custom_del_old_keys, score_table)


# todo save them using current
# todo only use one partition
# but in own table not window
# or all in heap?
# remove elements on window close, using window delta?

# , key_index=True
# https://github.com/robinhood/faust/issues/473

# run every 5 min to deuce score score *= 0.9
# this way a newer tweet has more priority

@app.agent(processed_topic, sink=[aggregated_topic])
async def aggregate(events):
    """ aggregate events

    Arguments:
        events: events from topic
    """
    time_last_publish = time.time()
    async for event in events.group_by(Event.obj_id):
        # count_table[event.obj_id] += 1
        score_table[event.obj_id] += event.get_score()
        if time.time() - time_last_publish > 2:
            time_last_publish = time.time()
            # yield trending.get_json()
            time_event = time.time()
            list_of_trends = list(score_table.relative_to_now().items().delta(timedelta(minutes=60 * time_main)))
            result = get_json(sorted(list_of_trends, key=lambda k: k[1], reverse=True)[:15])
            # print(len(list_of_trends) / ((time.time() - time_event) * 1000))
            yield result
            #     'trends': l,
            # }
        # check if obj_id in heap
        # true update
        # false

        # check if score is higher than smallest
        # true remove smallest, add new
        # false nothing

        # print(event.obj_id)
        # print(count_table[event.obj_id].current())
        # print(score_table[event.obj_id].current())


# @app.agent(topic_to_read_from, sink=[destination_topic])
# async def fetch(records):
#     async for record in records:
#         result = do_something(record)
#         yield result
if __name__ == '__main__':
    time.sleep(11)
    app.main()
