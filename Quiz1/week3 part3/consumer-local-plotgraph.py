from confluent_kafka import Consumer
import pandas as pd

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['streams-pageviewstats-typed-output'])

import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
figure(num=None, figsize=(10, 6), dpi=80, facecolor='w', edgecolor='k')
plt.ion()

lr = {}
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    value = msg.value()
    if value is None:
        value = 0
    else:
        value = msg.value()[-1]
        
    kvalue = msg.key().decode('utf-8')
    print('Received message: {0} , {1}'.format(kvalue, value))
   
    df = pd.DataFrame(d['sell'])

    df.plot(x='Quantity', y='Rate')

plt.show(block=True)
c.close()

