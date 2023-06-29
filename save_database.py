import ast

from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
import time
from Neo4j import Neo4jConnection

start_time = time.time()

spark = SparkSession.builder.config("spark.driver.memory", "16g").config("spark.driver.maxResultSize", "8g").config("spark.sql.legacy.timeParserPolicy", "LEGACY").appName("socialSummarization").getOrCreate()

subset_users_tweets_with_topics_df = spark.read.parquet("df_with_cat_and_topics/df_with_cat_and_topics.parquet")

subset_users_tweets_with_topics_df = subset_users_tweets_with_topics_df.withColumn("topicDistribution", vector_to_array("topicDistribution"))

pandas_subset_users_df = subset_users_tweets_with_topics_df.toPandas()

conversion_time = time.time() - start_time
print("Tempo di conversione del dataframe in Pandas: "+str(conversion_time))

conn = Neo4jConnection.Neo4jConnection("neo4j+ssc://dc9ef339.databases.neo4j.io", "neo4j", "nrpAFPs-NIfOqCatsL054RkZK3n_kUN7_UjYJHyg3fs")

def create_database():
    count_query = 0
    for i, row in pandas_subset_users_df.iterrows():

        array_mentionsn = ast.literal_eval(row["mentionsn"])
        array_mentionid = ast.literal_eval(row["mentionid"])
        res = {}
        for key in array_mentionid:
            for value in array_mentionsn:
                res[key] = value
                array_mentionsn.remove(value)
                break

        new_res = {str(k): v for k, v in res.items()}

        topic_dict = {
            "Presidenza di Trump": row['topicDistribution'][0],
            "Campagna presidenziale": row['topicDistribution'][1],
            "Confronto elettorale": row['topicDistribution'][2],
            "Impatto delle elezioni": row['topicDistribution'][3],
            "Caso Georgia": row['topicDistribution'][4]
        }

        parametri = {"tweetid": row['tweetid'],
                     "userid": row["userid"],
                     "screen_name": row["screen_name"],
                     "date": row["date"],
                     "lang": row["lang"],
                     "description": row["description"],
                     "tweet_type": row["tweet_type"],
                     "friends_count": row["friends_count"],
                     "listed_count": row["listed_count"],
                     "followers_count": row["followers_count"],
                     "favourites_count": row["favourites_count"],
                     "statuses_count": row["statuses_count"],
                     "verified": row["verified"],
                     "hashtag": ast.literal_eval(row["hashtag"]),
                     "urls_list": ast.literal_eval(row["urls_list"]),
                     "profile_pic_url": row["profile_pic_url"],
                     "profile_banner_url": row["profile_banner_url"],
                     "display_name": row["display_name"],
                     "date_first_tweet": row["date_first_tweet"],
                     "account_creation_date": row["account_creation_date"],
                     "mentionsn": new_res,
                     "cat": row["cat"],
                     "text": row["rt_text"] if row["tweet_type"] == "retweeted_tweet_without_comment" else row["text"],
                     "topic_0_prob": row['topicDistribution'][0],
                     "topic_1_prob": row['topicDistribution'][1],
                     "topic_2_prob": row['topicDistribution'][2],
                     "topic_3_prob": row['topicDistribution'][3],
                     "topic_4_prob": row['topicDistribution'][4]
                     }

        #print(type(parametri))
        #print(parametri)

        query = '''MERGE (tweet:Tweet{tweetid: $tweetid})
                   SET tweet.text = $text,
                       tweet.date = $date,
                       tweet.tweet_type = $tweet_type,
                       tweet.friends_count = $friends_count,
                       tweet.listed_count = $listed_count,
                       tweet.followers_count = $followers_count,
                       tweet.favourites_count = $favourites_count,
                       tweet.statuses_count = $statuses_count,
                       tweet.topic_0_prob = $topic_0_prob,
                       tweet.topic_1_prob = $topic_1_prob,
                       tweet.topic_2_prob = $topic_2_prob,
                       tweet.topic_3_prob = $topic_3_prob,
                       tweet.topic_4_prob = $topic_4_prob

                   MERGE (user:User {userid: $userid})
                   SET user.screen_name = $screen_name,
                       user.description = $description,
                       user.lang = $lang,
                       user.friends_count = $friends_count,
                       user.listed_count = $listed_count,
                       user.followers_count = $followers_count,
                       user.favourites_count = $favourites_count,
                       user.statuses_count = $statuses_count,
                       user.verified = $verified,
                       user.profile_pic_url = $profile_pic_url,
                       user.profile_banner_url = $profile_banner_url,
                       user.display_name = $display_name,
                       user.date_first_tweet = $date_first_tweet,
                       user.account_creation_date = $account_creation_date,
                       user.cat = $cat
                   MERGE (user)-[:POSTS]->(tweet)

                   FOREACH (tag IN $hashtag |
                       MERGE (hashtag:Hashtag {name:toLower(tag)})
                       MERGE (hashtag)-[:TAGS]->(tweet)
                   )

                   FOREACH (key IN keys($mentionsn) |
                       MERGE (mentioned:User {userid: key})
                       ON CREATE SET mentioned.screen_name = $mentionsn[key]
                       MERGE (tweet)-[:MENTIONS]->(mentioned)
                   )

                   FOREACH (link IN $urls_list |
                       FOREACH (key IN [key IN keys(link) WHERE key = 'expanded_url'] |
                           MERGE (url:Link {name: link[key]})
                           MERGE (tweet)-[:CONTAINS]->(url)
                       )
                   )

                   '''
        result = conn.query(query, parametri)
        count_query = count_query+1
        print("Query " + str(count_query) + " processata con successo")

start_creation_db_time = time.time()

create_database()

end_creation_db_time = time.time()

conn.close()

creation_db_time = end_creation_db_time - start_creation_db_time
print("Tempo di creazione del database in Neo4j: "+str(creation_db_time))



