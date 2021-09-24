from confluent_kafka import Producer
import time

import pandas as pd

# p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})
p = Producer({'bootstrap.servers': 'ec2-13-229-46-113.ap-southeast-1.compute.amazonaws.com:9092'}) # AWS AJ.POK

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        print('Message delivered to {}'.format(msg.value().decode('utf-8')))


#### SET INPUT TOPIC #####
# set_input_topic = 'streams-plaintext-input'
set_input_topic = '404-not-found'
set_input_topic2 = '404-not-found-2'

print("Start producer...")

### READ ALL FILE -- HARRY ####
file1 = open('Harry-Potter.txt', 'r', encoding = 'utf-8')
Lines = file1.readlines()
###############################

Page_line  = []

Page = 2 # Set Start Page
for line in Lines:
    # p.poll(0)
    
    if '|' in line:
        for i in line.split():
            if i.isdigit():
                Page = int(i)+1
    
    line = line.replace("\n", " ")
    Page_line.append((Page,line))
# print(Page_line)

df_pageLine = pd.DataFrame(Page_line,columns =['Page','Line'])

df_pageLine_temp = df_pageLine

df_pageLine_temp['Line'] = df_pageLine_temp.groupby(['Page'])['Line'].transform(lambda x: ' '.join(x))
df_pageLine_temp = df_pageLine_temp.drop_duplicates()

df_pageCountLine = df_pageLine.groupby(['Page']).count().reset_index()

pageSentence = df_pageLine_temp.to_records(index=False)
pageSentence = list(pageSentence)
pagecountLine = df_pageCountLine.to_records(index=False)
pagecountLine = list(pagecountLine)

for i in pageSentence:
    x,y  = i
    kMsg = str(x)
    kMsg = kMsg.encode().decode('utf-8')
    sendMsg = y
    sendMsg = sendMsg.encode().decode('utf-8')
    # sendMsg = sendMsg.encode().decode('utf-8').strip('\n')

    ##### (page, sentence) #####
    p.produce(set_input_topic, key = kMsg, value = sendMsg, callback=delivery_report)
    print(kMsg,":",sendMsg)

    ##### count line #####
    # sendMsg2 = pagecountLine[x][1]
    # p.produce(set_input_topic2, sendMsg2, callback=delivery_report)
    # print(sendMsg2)

    # time.sleep(1)


# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

    # r1 = {"user":line, "timestamp": int(time.time()*1000)}
    # p.produce('streams-pageview-input', key=line, value=json.dumps(r1))
    # p.produce('streams-plaintext-input', line)

