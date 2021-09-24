from confluent_kafka import Producer
import time

p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        print('Message delivered to {}'.format(msg.value().decode('utf-8')))


# import json
# import random

# i = 0

#### SET INPUT TOPIC #####
set_input_topic = 'streams-plaintext-input'
# set_input_topic = 'streams-plaintext-input'

print("Start producer...")

# with open('Harry-Potter.txt','r',encoding="utf8") as f:
#     content = f.readlines()

### READ ALL FILE -- HARRY ###
file1 = open('Harry-Potter.txt', 'r', encoding = 'utf-8')
Lines = file1.readlines()
###########################

for line in Lines:
    # print(line)
    p.poll(0)
    # r1 = {"user":line, "timestamp": int(time.time()*1000)}
    # p.produce('streams-pageview-input', key=line, value=json.dumps(r1))
    # p.produce('streams-plaintext-input', line)
    sendMsg = line.encode().decode('utf-8').strip('\n')
    p.produce(set_input_topic, sendMsg , callback=delivery_report)

    time.sleep(1)
    print(line)

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