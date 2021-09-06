from confluent_kafka import Producer
import time

p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# uList = ["user1","user2"]
# pList = ["page1","page2","page3","page4","page5"]
# rList = ["asia","europe","america"]


import json
import random

# Trigger any available delivery report callbacks from previous produce() calls
# p.poll(0)
i = 0
print("Start producer...")

with open('Harry-Potter.txt','r',encoding="utf8") as f:
    content = f.readlines()
    # print(content)

for line in content:
    # print(line)
    p.poll(0)
    r1 = {"user":line, "timestamp": int(time.time()*1000)}
    p.produce('streams-pageview-input', key=line, value=json.dumps(r1))
    time.sleep(1)

# while True:
# # for i in range(3):
#     p.poll(0)
#     user = random.choice(uList)
#     page = random.choice(pList)
#     region = random.choice(rList)
#     # region = rList[i]
#     # for i in rList:
#     #     region = i
#     r1 = {"_t": "pv", "user":user, "page":page, "timestamp": int(time.time()*1000)}
#     r2 = {"_t": "up", "region":region, "timestamp": int(time.time()*1000)}
#     print(r1)
#     print(r2)
#     p.produce('streams-pageview-input', key=user, value=json.dumps(r1))
#     p.produce('streams-userprofile-input', key=user, value=json.dumps(r2))
#     time.sleep(1)
#     '''i = i+1
#     print(i)
#     if i == 5:
#         break'''
#     # i += 1
#     # print(i)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()