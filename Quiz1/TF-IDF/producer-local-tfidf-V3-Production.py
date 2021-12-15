from confluent_kafka import Producer
import time

import re
import nltk
from nltk.corpus import stopwords

stopWord = stopwords.words('english')

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

print("Start producer...")

### READ ALL FILE -- HARRY #########
file1 = open('Harry-Potter.txt', 'r', encoding = 'utf-8')
Lines = file1.read()
####################################

Lines = Lines.replace("\n", " ")
wholeBook = Lines.split('|')
# wholeBook = Lines.split('Harry Potter and the Philosophers Stone - J.K. Rowling')

run_page = 1
# for sentence in wholeBook[:-1]:
for sentence in wholeBook[:10]:
    SentenceClean = ''
    sentence = sentence.lower()
    find_words = re.compile(r'\w+').findall(sentence)
    for i in find_words:
        if i not in stopWord:
            SentenceClean += i + " "
    run_page += 1
    page = str(run_page)
    p.poll(0)

    kMsg = page.encode().decode('utf-8')
    sendMsg = SentenceClean.encode().decode('utf-8')
    # sendMsg = sentence.encode().decode('utf-8').strip('\n')
    p.produce(set_input_topic, key=kMsg, value=sendMsg , callback=delivery_report)
    print(kMsg,":",sendMsg)
    # time.sleep(5)


# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

    # r1 = {"user":line, "timestamp": int(time.time()*1000)}
    # p.produce('streams-pageview-input', key=line, value=json.dumps(r1))
    # p.produce('streams-plaintext-input', line)
