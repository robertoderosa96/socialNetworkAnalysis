from pyspark.sql import SparkSession
import time
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import *
from pyspark.ml.feature import CountVectorizer, StopWordsRemover
from pyspark.ml.clustering import LDA

start_time = time.time()

#Creazione di una sessione Spark che utilizza tutta la RAM a disposizione
spark = SparkSession.builder.config("spark.driver.memory", "16g").config("spark.driver.maxResultSize", "8g").appName("socialSummarization").getOrCreate()

#Lettura dei dataset dei tweet da HDFS
df_tweets = spark.read.csv('hdfs://localhost:9000/projectsFedericoII/socialSummarization/tweets/', header=True, sep=",", inferSchema=True, multiLine=True, quote="\"", escape="\"")

read_time = time.time() - start_time
print("Tempo di lettura del dataframe: "+str(read_time)+"s")

"""
START: PULIZIA DEL TESTO DEI TWEET
"""
#Funzione che rimuove i link dai tweet
def remove_links(tweet):
    tweet = re.sub(r'http\S+', '', tweet)
    tweet = re.sub(r'bit.ly/\S+', '', tweet)
    tweet = tweet.strip('[link]')
    return tweet

#Funzione che rimuove la sottostringa "RT @screen_name" da tutti i retweet o dai quoted tweet
def remove_rt(tweet):
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet

#Funzione che rimuove la punteggiatura dai tweet
def remove_punctuation(tweet):
    tweet = re.sub('[!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@#â]+', '', tweet)
    return tweet

#Funzione che rimuove le emoji dai tweet
def remove_emoji(string):
    emoji_pattern_path = "emoji/emoji_pattern"
    emoji_pattern_file = open(emoji_pattern_path, 'r')
    emoji_pattern = emoji_pattern_file.read()
    emoji_pattern = re.compile(emoji_pattern, flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

#Dichiarazinoe dei path ai file che contengono le stopwords
english_stopwords_file_path = "stopwords/english_stopwords"
deutsch_stopwords_file_path = "stopwords/deutsch_stopwords"
french_stopwords_file_path = "stopwords/french_stopwords"
portuguese_stopwords_file_path = "stopwords/portuguese_stopwords"
italian_stopwords_file_path = "stopwords/italian_stopwords"

#Lettura delle stopwords inglesi dal file corrispondente
with open(english_stopwords_file_path, 'r') as f:
    en_stopwords_set = set(f.read().splitlines())
#Lettura delle stopwords tedesche dal file corrispondente
with open(deutsch_stopwords_file_path, 'r') as f:
    de_stopwords_set = set(f.read().splitlines())
#Lettura delle stopwords francesi dal file corrispondente
with open(french_stopwords_file_path, 'r') as f:
    fr_stopwords_set = set(f.read().splitlines())
#Lettura delle stopwords portoghesi dal file corrispondente
with open(portuguese_stopwords_file_path, 'r') as f:
    pt_stopwords_set = set(f.read().splitlines())
#Lettura delle stopwords italiane dal file corrispondente
with open(italian_stopwords_file_path, 'r') as f:
    it_stopwords_set = set(f.read().splitlines())

# Funzione per rimuovere le stopwords da un array di stringhe
@udf(returnType=ArrayType(StringType()))
def remove_stopwords(tokens):
    return [word for word in tokens if word.lower() not in (en_stopwords_set or de_stopwords_set or fr_stopwords_set or pt_stopwords_set or it_stopwords_set)]

#Dichiarazioni delle funzioni sopra come UDF
remove_links=udf(remove_links)
remove_rt=udf(remove_rt)
remove_punctuation=udf(remove_punctuation)
remove_emoji = udf(remove_emoji, StringType())

df_tweets = df_tweets.withColumn('text', remove_links(df_tweets['text']))
df_tweets = df_tweets.withColumn('text', remove_rt(df_tweets['text']))
df_tweets = df_tweets.withColumn('text', remove_punctuation(df_tweets['text']))
df_tweets = df_tweets.withColumn('text', remove_emoji(df_tweets['text']))
"""
END: PULIZIA DEL TESTO DEI TWEET SCRITTI DAGLI UTENTI
"""

#Tokenizzazione del testo dei tweet e rimozione delle stopwords
tokenizer = RegexTokenizer().setPattern("[\\W_]+").setMinTokenLength(3).setInputCol("text").setOutputCol("tokens")
df_tweets = tokenizer.transform(df_tweets)
df_tweets = df_tweets.withColumn('tokens_without_stopwords', remove_stopwords(df_tweets['tokens']))

lda_start_time = time.time()
"""
######################################################################################################################################
                                                              START
######################################################################################################################################
############################################################## LDA ###################################################################
######################################################################################################################################
"""
count_vectorizer = CountVectorizer(inputCol="tokens_without_stopwords", outputCol="features")
model = count_vectorizer.fit(df_tweets)
dataframe_vectorized = model.transform(df_tweets)
dataframe_vectorized.show(20, truncate=False)

num_topics = 5
lda = LDA(k=num_topics, maxIter=300)
model_lda = lda.fit(dataframe_vectorized)

vocab = model.vocabulary
topics = model_lda.describeTopics(maxTermsPerTopic=15)
a=topics.rdd.map(lambda row: row['termIndices']).collect()
print(a)
b=topics.rdd.map(lambda row: row['termIndices']).map(lambda idx_list: [vocab[idx] for idx in idx_list]).collect()
print(b)
topics.show(truncate=False)

transformed = model_lda.transform(dataframe_vectorized)
transformed.show(10, truncate=False)
"""
######################################################################################################################################
                                                              END
######################################################################################################################################
############################################################# LDA ###################################################################
######################################################################################################################################
"""
lda_time = time.time() - lda_start_time
print("Tempo di esecuzione LDA: "+str(lda_time)+"s")

#Creazione di un dataframe contenente id del tweet e punteggi per ogni topic
tweets_topics_df = transformed.select("tweetid", "topicDistribution")

#Salvataggio dell dataFrame su file in formato Parquet
tweets_topics_df.write.format("parquet").save("tweets_topic/tweets_topic.parquet")

execution_time = time.time() - start_time
print("Tempo di esecuzione totale: "+str(execution_time)+"s")