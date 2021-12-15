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

time.time()

while True:
    msg = c.poll(0)
    # print(msg)

    if msg is None:
        # print('Received message: harry , 0')
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    
    #############################################################
    value = int.from_bytes(msg.value(),"big")
        
    kvalue = msg.key().decode("utf-8", "ignore")

    if "harry" in kvalue:
        print('Received message: {0} , {1}'.format("harry", value))
    else:
        print('Received message: {0} , {1}'.format("harry", 0))

    # print('Received message: {0} , {1}'.format(kvalue, value))
    #############################################################
    print(time.time())
    
    # print(kvalue, type(value), value)
    
    # time.sleep(1)
    
    # print(Harry_count(kvalue))
    
    # , {1}'.format(kvalue, value))
    
    
c.close()

