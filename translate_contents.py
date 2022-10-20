import pymysql
import pandas as pd
import threading
from textblob import TextBlob

connection = pymysql.connect(
    host='localhost',  # 127.0.0.1
    user='root',
    password='12345678',
    database='ds'
)
df = pd.read_sql("select * from sentiment_data", connection)

data = pd.DataFrame(df.content)

translations = []


# Creating a new instance of textblob for each content
# and calling its translate function

def translate(start, end):
    for i in range(start, end):
        text = data.iloc[i].content.strip()
        if len(text) == 0:
            continue
        blob = TextBlob(text)
        blob_eng = blob.translate(from_lang='az', to='en')
        translations.append([i, blob_eng])
        print(i)


low = 0
all_threads = []
NUMBER_OF_CONTENT = 5000
THREAD_COUNT = 6

# Since I have 6 cores, 6 threads will be the most optimum
# And here I assign 5000 content to each thread
for i in range(THREAD_COUNT):
    thread = threading.Thread(target=translate, args=(low, low + NUMBER_OF_CONTENT))
    low += NUMBER_OF_CONTENT
    all_threads.append(thread)
    thread.start()

# I want to put all the translations in a dataframe so,we have to wait all the threads to be completed
for thread in all_threads:
    thread.join()

# Pandas is not thread safe that's why I did it implicitly (list is thread safe)
for translation in translations:
    data._set_value(translation[0], 'content', value=str(translation[1]))

data.to_pickle("news_english.pkl")
