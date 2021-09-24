from confluent_kafka import Producer
import time

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
set_input_topic = 'testtopic'

print("Start producer...")

### READ ALL FILE -- HARRY ###
file1 = open('Harry-Potter.txt', 'r', encoding = 'utf-8')
Lines = file1.readlines()
###########################
wholeBook = []
Page = 2 # Set Start Page
for line in Lines:
    p.poll(0)

    if 'Harry Potter and the Philosophers Stone - J.K. Rowling' in line:
        for i in line.split():
            if i.isdigit():
                Page = int(i)+1
    
    line = line.replace("\n", " ")
    kpageMsg = str(Page).encode().decode('utf-8')
    sendMsg = line.encode().decode('utf-8').strip('\n')

    Tuple_Line = (kpageMsg,sendMsg)
    wholeBook.append(Tuple_Line)
    # time.sleep(1)
    # print(kpageMsg , sendMsg)
    # p.produce(set_input_topic, key = kpageMsg, value = sendMsg, callback=delivery_report)

wholeBook = str(wholeBook).encode().decode('utf-8')
# wholeBook = bytes(wholeBook)

print(wholeBook)
run_num = 0
while True:
    
    if run_num != 10000:
        time.sleep(2)
        p.produce(set_input_topic, wholeBook, callback=delivery_report)
        print(run_num)
    else:
        print(run_num)
        break

    run_num += 1


# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

    # r1 = {"user":line, "timestamp": int(time.time()*1000)}
    # p.produce('streams-pageview-input', key=line, value=json.dumps(r1))
    # p.produce('streams-plaintext-input', line)
