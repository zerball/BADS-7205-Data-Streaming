from confluent_kafka import Consumer
import time

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

# c.subscribe(['streams-pageviewstats-untyped-output'])
c.subscribe(['streams-wordcount-output'])              


# def Harry_count(str):
#     #counts = dict()
#     count = 0
#     harry = 'Harry'
#     sentence = str.split()

#     for word in sentence:
#         if harry in sentence:
#             # counts[word] += 1
#             count += 1    
#         else:
#             # counts[word] = 1
#             pass
    
#     return count

print("Start consumer...")

while True:
    msg = c.poll(0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    # print('---'*10)
    # print(msg)

    value = msg.value().decode()
        
    kvalue = msg.key().decode("utf-8", "ignore")

    print(kvalue, value)
    # print(Harry_count(kvalue))
    
    # , {1}'.format(kvalue, value))
    
    
c.close()

