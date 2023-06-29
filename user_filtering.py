from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import ast
from Neo4j import Neo4jConnection

start_time = time.time()

#Creazione di una sessione Spark che utilizza tutta la RAM a disposizione
spark = SparkSession.builder.config("spark.driver.memory", "16g").config("spark.driver.maxResultSize", "8g").config("spark.sql.legacy.timeParserPolicy", "LEGACY").appName("socialSummarization").getOrCreate()

#Lettura dei vari dataset dei tweets da HDFS
df_tweets = spark.read.csv('hdfs://localhost:9000/projectsFedericoII/socialSummarization/tweets/', header=True, sep=",", inferSchema=True, multiLine=True, quote="\"", escape="\"")

#Funzione per catturare i link YouTube dalla colonna "urls_list "e per costruire una lista di link YouTube
def filter_expanded_url(urls_list):
    filtered_list = []
    try:
        urls_list = ast.literal_eval(urls_list)
        if isinstance(urls_list, list):
            for url_dict in urls_list:
                expanded_url = url_dict.get('expanded_url', '')
                if expanded_url.startswith('https://www.youtube.com/') or expanded_url.startswith('https://youtu.be/') or expanded_url.startswith('http://www.youtube.com/') or expanded_url.startswith('http://youtu.be/'):
                    filtered_list.append(url_dict)
    except (SyntaxError, ValueError):
        pass
    return filtered_list

#Registrazione della funzione filter_expanded_url come UDF
filter_expanded_url_udf = udf(filter_expanded_url, ArrayType(StringType()))

#Conteggio dei link YouTube per ciascun utente
filtered_df = df_tweets.withColumn('urls_list', filter_expanded_url_udf(col('urls_list')))
filtered_df = filtered_df.filter(col('urls_list').isNotNull() & (size(col('urls_list')) > 0))
count_user_yt_shares = filtered_df.groupBy("userid").agg(count('*').alias('yt_shares_count'))

#Conteggio degli hashtag per ciascun utente
exploded_hashtag_df = df_tweets.withColumn("hashtag", explode(split("hashtag", ", ")))
count_hashtag_df = exploded_hashtag_df.groupBy("userid").agg(count("hashtag").alias("hashtags_count"))

#Conteggio dei tweet per ciascun utente
tweets_count_df = df_tweets.groupBy("userid").agg(count('*').alias('tweets_count'))
count_tweets_df = tweets_count_df.filter((tweets_count_df["tweets_count"] >= 500) & (tweets_count_df["tweets_count"] <= 800))

#Unione del dataframe che possiede i count dei tweets con il dataframe che possiede i count degli hashtag
joined_df1 = count_hashtag_df.join(count_tweets_df, on="userid", how="inner")

#Unione del dataframe sopra  con il dataframe che possiede i count dei link YouTube
joined_df2 = joined_df1.join(count_user_yt_shares, on="userid", how="inner").orderBy(desc('tweets_count'), desc('yt_shares_count'), desc('hashtags_count'))

#Definizione un limite di utenti tale da non saturare i limiti gratuiti di Neo4j e AuraDB
user_limit = 50

"""
   Costruzione di un dataframe con le colonne "userid", "hashtags_count", "tweets_count", "yt_shares_count" limitato a 50 utenti
   Dataframe che ordina i primi 50 utenti in ordine decrescente in questo ordine:
   - Numero di Tweet
   - Numero di Hashtag
   - Numero di link YouTube
"""
users_rank = joined_df2.limit(user_limit)

rank_time = time.time() - start_time
print("Tempo per la costruzione della classifica dei primi TOT utenti: "+str(rank_time)+"s")

#Lettura del dataset degli utenti
df_users = spark.read.csv('hdfs://localhost:9000/projectsFedericoII/socialSummarization/users/', header=True, sep=",", inferSchema=True)

#Creazione di un dataframe di tweet che possiede solo i tweet dei 50 utenti ricavati nel dataframe users_rank
subset_tweets_df = df_tweets.join(users_rank, on="userid")
subset_users_tweets_df = subset_tweets_df.join(df_users, subset_tweets_df['screen_name'] == df_users['user'], how="left").drop(df_users['user'])

#Lettura del dataframe contenente i tweet con i punteggi dei topic creato in precedenza con l'algoritmo LDA:
tweets_with_topics = spark.read.parquet("tweets_topic/tweets_topic.parquet")

#Creazione di un dataframe contenente tutti i tweet dei 50 utenti, con l'aggiunta della colonna che contiene i punteggi dei topic
subset_users_tweets_with_topics_df = subset_users_tweets_df.join(tweets_with_topics, subset_users_tweets_df["tweetid"] == tweets_with_topics["tweetid"]).drop(tweets_with_topics["tweetid"])

#Casting della colonna reply_status_id in long
subset_users_tweets_with_topics_df = subset_users_tweets_with_topics_df.withColumn("reply_statusid", col("reply_statusid").cast("long"))

#Casting delle colonne contenenti le date del primo tweet dell'utente e della creazione dell'account in formato DATE
subset_users_tweets_with_topics_df = subset_users_tweets_with_topics_df.withColumn("date_first_tweet", to_timestamp(subset_users_tweets_with_topics_df.date_first_tweet, "EEE MMM dd HH:mm:ss Z yyyy"))
subset_users_tweets_with_topics_df = subset_users_tweets_with_topics_df.withColumn("date_first_tweet", date_format(subset_users_tweets_with_topics_df.date_first_tweet, "EEE MMM dd HH:mm:ss Z yyyy"))
subset_users_tweets_with_topics_df = subset_users_tweets_with_topics_df.withColumn("account_creation_date", to_timestamp(subset_users_tweets_with_topics_df.account_creation_date, "EEE MMM dd HH:mm:ss Z yyyy"))
subset_users_tweets_with_topics_df = subset_users_tweets_with_topics_df.withColumn("account_creation_date", date_format(subset_users_tweets_with_topics_df.account_creation_date, "EEE MMM dd HH:mm:ss Z yyyy"))

#Memorizzazione del dataframe dei tweet con i punteggi dei topic in formato Parquet
subset_users_tweets_with_topics_df.write.format("parquet").save("df_with_cat_and_topics/df_with_cat_and_topics.parquet")

execution_time = time.time() - start_time
print("Tempo per l'esecuzione dello script: "+str(execution_time)+"s")

