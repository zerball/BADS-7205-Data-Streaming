import nltk
tokenizer = nltk.RegexpTokenizer(r"\w+")

def Harry_count(str):
    #counts = dict()
    count = 0
    # Harry_collection = ['Harry','Harry!','Harry.','Harry,','Harry?','Harry ']
    sentence = tokenizer.tokenize(str)

    for word in sentence:
        if word == 'Harry':
            count += 1    
        else:
            pass
    
    return count

# with open('Harry-Potter.txt','r',encoding="utf8") as f:
#     content = f.readlines()
#     # print(content)

# for line in content:
#     # print(line)
#     # p.poll(0)
#     # r1 = {"user":line, "timestamp": int(time.time()*1000)}
#     # p.produce('streams-pageview-input', key=line, value=json.dumps(r1))
#     # time.sleep(1)
#     print(line)
#     print(Harry_count(line))
#     print('-'*10000)

# test = '''
# Harry fron line 1/n
# Harry fron line 2/n
# Harry fron line 3/n
# Harry fron line 4/n
# Harry fron line 5

# '''
for i in range(3):
    k = input('sentence : ')
    print(f"Count 'Harry' word : {Harry_count(k)}")