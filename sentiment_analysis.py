from Neo4j import Neo4jConnection
import re
from googletrans import Translator
import time

#START: Import necessari al modello per la sentiment analysis
from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax
#END: Import necessari al modello per la sentiment analysis

start_time = time.time()

conn = Neo4jConnection.Neo4jConnection("neo4j+ssc://dc9ef339.databases.neo4j.io", "neo4j", "nrpAFPs-NIfOqCatsL054RkZK3n_kUN7_UjYJHyg3fs")

query_get_tweets_data = 'MATCH (user:User)-[:POSTS]->(tweet:Tweet) RETURN tweet.tweetid AS tweetid, tweet.text AS text, user.lang AS lang'

tweets_data = conn.query(query_get_tweets_data)

def remove_links(tweet_text):
    tweet_text = re.sub(r'http\S+', '', tweet_text)
    tweet_text = re.sub(r'bit.ly/\S+', '', tweet_text)
    tweet_text = tweet_text.strip('[link]')
    return tweet_text

def remove_rt(tweet_text):
    tweet_text = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet_text)
    return tweet_text

def remove_emoji(tweet_text):
    emoji_pattern_path = "emoji/emoji_pattern"
    emoji_pattern_file = open(emoji_pattern_path, 'r')
    emoji_pattern = emoji_pattern_file.read()
    emoji_pattern = re.compile(emoji_pattern, flags=re.UNICODE)
    return emoji_pattern.sub(r'', tweet_text)

translator = Translator()

def translate_to_english(tweet_text, tweet_lang):
    translation = translator.translate(tweet_text, src=tweet_lang, dest='en')
    return translation.text

MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)

def sentiment_analysis(tweet_text):
    encoded_input = tokenizer(tweet_text, return_tensors='pt')
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    ranking = np.argsort(scores)
    ranking = ranking[::-1]
    scores_dict = {}
    for i in range(scores.shape[0]):
        l = config.id2label[ranking[i]]
        s = scores[ranking[i]]
        rounded_score = np.round(float(s), 4)
        if(l == "positive"):
            scores_dict['positive'] = rounded_score
        if (l == "neutral"):
            scores_dict['neutral'] = rounded_score
        if (l == "negative"):
            scores_dict['negative'] = rounded_score
    return scores_dict

count_query = 0

for result in tweets_data:
    tweet_id = result['tweetid']
    tweet_lang = result['lang']
    tweet_text = result['text']

    tweet_text = remove_links(tweet_text)
    tweet_text = remove_rt(tweet_text)
    tweet_text = remove_emoji(tweet_text)

    if (tweet_lang != "en" and tweet_lang!="und" and tweet_text != ""):
        tweet_text = translate_to_english(tweet_text, tweet_lang)

    tweet_sa_scores_dict = sentiment_analysis(tweet_text)
    query_parameters_dict = {
        'tweetid': tweet_id,
        'positive_score': tweet_sa_scores_dict['positive'],
        'neutral_score': tweet_sa_scores_dict['neutral'],
        'negative_score': tweet_sa_scores_dict['negative']
    }
    query_set_sa_scores = 'MATCH (tweet:Tweet) WHERE tweet.tweetid = $tweetid SET tweet.sa_positive_score = $positive_score, tweet.sa_neutral_score = $neutral_score, tweet.sa_negative_score = $negative_score'
    result = conn.query(query_set_sa_scores, query_parameters_dict)

    count_query = count_query + 1
    print("Query " + str(count_query) + " eseguita con successo")

sentiment_analysis_time = time.time() - start_time
print("Tempo per la sentiment analysis: "+str(sentiment_analysis_time))

conn.close()