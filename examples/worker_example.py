import redis
from simplekiq import KiqQueue
from simplekiq import EventBuilder
from simplekiq import Worker


class MyEventWorker(Worker):
    def __init__(self, queue, failed_queue):
        super().__init__(queue, failed_queue)

    def _process(self, event_type, value):
        print(event_type, value)


queue = KiqQueue("127.0.0.1:6379", "api_worker", True)
failed_queue = KiqQueue("127.0.0.1:6379", "api_failed", True)

#event_builder = EventBuilder(queue)
#value = event_builder.emit("test_event", {"age": 13, "value": "test", "1": {"2": 2}}, 3)
#queue.enqueue(value)

worker = MyEventWorker(queue, failed_queue)

while True:
    worker.process(True)
