import json
import time
import string
import random
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def gen_message():
    return {
        "id": random.randint(1e9, 1e10),
        "time": int(time.time()),
        "msg": ''.join(random.choices(string.ascii_uppercase + string.digits, k=100))
    }

for _ in range(100):
    data = gen_message()
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('topic1', json.dumps(data).encode('utf-8'), callback=delivery_report)
    p.produce('topic2', str(data["id"]).encode('utf-8'), callback=delivery_report)
    time.sleep(random.random()*2)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()