import re
import nltk
from nltk.corpus import stopwords

stopWord = stopwords.words('english')

### READ ALL FILE -- HARRY #########
file1 = open('Harry-Potter.txt', 'r', encoding = 'utf-8')
Lines = file1.read()
####################################

Lines = Lines.replace("\n", " ")
wholeBook = Lines.split('|')
# wholeBook = Lines.split('Harry Potter and the Philosophers Stone - J.K. Rowling')

run_page = 1
for sentence in wholeBook[:-1]:
    SentenceClean = ''
    sentence = sentence.lower()
    find_words = re.compile(r'\w+').findall(sentence)
    for i in find_words:
        if i not in stopWord:
            SentenceClean += i + " "

    run_page += 1
    page = str(run_page)
    print(SentenceClean)

    # print(kMsg,":",sendMsg)