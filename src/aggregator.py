import copy
import datetime
import time
import uuid
from datetime import timedelta

import heapdict
import faust

app = faust.App(
    'aggregator',
    broker='kafka:9092',
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


class TrendingHeap(object):
    """ use a heapdict of fixed size to store the top events
        more for heapdict: https://www.geeksforgeeks.org/priority-queue-using-queue-and-heapdict-module-in-python/
     """

    size = 10

    def __init__(self):
        """ init an empty heapdict """
        self.heap = heapdict.heapdict()

    def update(self, key, score):
        """ update the heapdict, return true if changes, false if not

        return true if full and update happened
         """
        if self.contains(key):
            self.heap[key] = score
            return self.is_full()
        else:
            if not self.is_full():
                self.heap[key] = score
                return self.is_full()
            else:
                if score > self.get_lowest_score():
                    self.heap.popitem()
                    self.heap[key] = score
                    return self.is_full()
        return False

    def get_items(self):
        """ return the item in the heap """
        return self.heap.items()

    def get_lowest_score(self):
        """ return the lowest score in this heap """
        return self.heap.peekitem()[1]

    def contains(self, key):
        """ check if an event is already in this heap """
        if key in self.heap:
            return True
        return False

    def is_empty(self):
        """ check if heap is empty """
        return len(self.heap) == 0

    def is_full(self):
        """ check if heap is full, meaning its length is the desired size """
        return len(self.heap) == self.size

    def get_json(self):
        heap_copy = copy.deepcopy(self.heap)
        result = {'id': str(uuid.uuid4()), 'relation_type': 'discusses', 'state': 'aggregated',
                  'created_at': '{0:%Y-%m-%dT%H:%M:%SZ}'.format(datetime.datetime.now())}
        aggregated_data = {}
        while heap_copy:
            item = heap_copy.popitem()
            aggregated_data[len(heap_copy)] = {
                'doi': item[0],
                'score': item[1]
            }
        result['aggregated_data'] = aggregated_data
        return result


processed_topic = app.topic('events_processed-discusses', value_type=Event)
aggregated_topic = app.topic('events_aggregated')
# count_table = app.Table('count_processed_events', default=int).hopping(10, 5, expires=timedelta(minutes=10))
# time in seconds, first is window size, second is time between creation
score_table = app.Table('score_processed_events', default=int).hopping(timedelta(minutes=60), timedelta(minutes=5), expires=timedelta(minutes=120))

trending = TrendingHeap()


# , key_index=True
# https://github.com/robinhood/faust/issues/473

@app.agent(processed_topic, sink=[aggregated_topic])
async def aggregate(events):
    """ aggregate events

    Arguments:
        events: events from topic
    """
    async for event in events.group_by(Event.obj_id):
        # count_table[event.obj_id] += 1
        score_table[event.obj_id] += event.get_score()
        if trending.update(event.obj_id, score_table[event.obj_id].current()):
            print('update')
            yield trending.get_json()
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
