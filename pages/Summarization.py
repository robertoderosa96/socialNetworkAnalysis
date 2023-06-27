import streamlit as st
from Neo4j import Neo4jConnection
import plotly.graph_objects as go
import pandas as pd
import openai
import re
import random

st.set_page_config(page_title='SSCN - Summarization', layout='wide', page_icon='📊')

openai.api_key = "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"

# Set connection global
if 'conn' not in st.session_state:
    st.session_state['conn'] = Neo4jConnection.Neo4jConnection("neo4j+ssc://dc9ef339.databases.neo4j.io", "neo4j", "nrpAFPs-NIfOqCatsL054RkZK3n_kUN7_UjYJHyg3fs")

# Set user list global
if 'screen_name_list' not in st.session_state:
    # Get all screen name from database
    query_get_all_users_screen_name = 'MATCH (user:User) WHERE user.verified IS NOT NULL RETURN user.screen_name AS screen_name'
    users_screen_names = st.session_state['conn'].query(query_get_all_users_screen_name)
    screen_name_list = []
    for item in users_screen_names:
        screen_name_list.append(item['screen_name'])
    st.session_state['screen_name_list'] = screen_name_list

# Set screen name global
if 'screen_name' not in st.session_state:
    st.session_state['screen_name'] = st.session_state['screen_name_list'][0]

# Sidebar layout
st.sidebar.title('Selezione utente')
st.sidebar.selectbox('Seleziona l\'utente per visualizzarne le analytics', options=st.session_state['screen_name_list'],
                        index=st.session_state['screen_name_list'].index(st.session_state['screen_name']), key='screen_name')

# Title
st.markdown("<h1 style='text-align: center; font-weight: bold;'>Tweets analytics during USA 2020 political elections</h1>", unsafe_allow_html=True)

selected_screen_name = {'screen_name': st.session_state['screen_name']}

# <!-- SUMMARIZATION -->

st.markdown("<h3 style='text-align: center; font-weight: bold;'>Seleziona un topic per generare un summary dei tweet</h3>", unsafe_allow_html=True)
col_button_1, col_button_2, col_button_3, col_button_4, col_button_5 = st.columns(5)

with col_button_1:
    selected_option1 = st.button('Presidenza di Trump', use_container_width=True)
with col_button_2:
    selected_option2 = st.button('Campagna Presidenziale', use_container_width=True)
with col_button_3:
    selected_option3 = st.button('Confronto elettorale', use_container_width=True)
with col_button_4:
    selected_option4 = st.button('Impatto delle Elezioni', use_container_width=True)
with col_button_5:
    selected_option5 = st.button('Caso Georgia', use_container_width=True)


query_get_tweets_and_topics = 'MATCH (user:User)--(tweet:Tweet) WHERE user.screen_name = $screen_name RETURN tweet.text AS tweet_text, tweet.topic_0_prob,tweet.topic_1_prob,tweet.topic_2_prob,tweet.topic_3_prob,tweet.topic_4_prob'
tweets_and_topics_probs = st.session_state.conn.query(query_get_tweets_and_topics, selected_screen_name)

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

#Funzione che dati in ingresso:
# - Lista dei singoli tweet con le relative probabilità dei topic
# - L'indice del topic
#Restituisce il testo unito di tutti i tweet per il topic corrispondente all'indice passato come parametro
def merge_tweet_text_for_topic(tweets_and_topics_probs, topic_index):
    topic_text_list = []
    for row in tweets_and_topics_probs:
        tweet_text = row.values()[0]
        topics_probs_list = [row.values()[1], row.values()[2], row.values()[3], row.values()[4], row.values()[5]]
        max_prob_topic = max(topics_probs_list)
        max_index_prob_topic = topics_probs_list.index(max_prob_topic)
        if max_index_prob_topic == topic_index:
            topic_text_list.append(tweet_text)
    delimiter = '\n'
    merged_topic_text = delimiter.join(topic_text_list)
    merged_topic_text = remove_links(merged_topic_text)
    merged_topic_text = remove_rt(merged_topic_text)
    merged_topic_text = remove_emoji(merged_topic_text)
    return merged_topic_text

def divide_in_batch(merged_text, batch_max_size):
    batches_list = []
    current_batch = ""
    tweet_text_list = merged_text.split("\n")
    for single_tweet_text in tweet_text_list:
        if len(current_batch + single_tweet_text + "\n") <= batch_max_size:
            current_batch += single_tweet_text + "\n"
        else:
            batches_list.append(current_batch)
            current_batch = single_tweet_text + "\n"
    if current_batch:
        batches_list.append(current_batch)

    return batches_list

def merge_selected_batches(batches):
    batches_number = len(batches)
    random_numbers_list = random.sample(range(batches_number), 12)
    selected_batches = []
    for random_number in random_numbers_list:
        selected_batches.append(batches[random_number])
    delimiter = '\n'
    merged_batches = delimiter.join(selected_batches)
    return merged_batches

batch_max_size = 1000

def generate_summary(merged_text):
    prompt = "Mi puoi generare un summary tra 100 e 150 parole, in italiano, di questi tweet evidenziando la preferenza politica dell'utente (Democratico o Repubblicano) e le sue caratteristiche"
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        temperature=1,
        max_tokens=500,
        top_p=1.0,
        frequency_penalty=0.5,
        presence_penalty=0.0,
        messages = [
            {
                "role": "user",
                "content": f"{prompt} Il testo da esaminare è il seguente: {merged_text}",
            },
        ],
    )
    summary_string_response = response["choices"][0]["message"]["content"]
    return summary_string_response

spinner = st.spinner("Generazione del summary...")

openAI_response = ""

with spinner:
    if selected_option1:
        topic_index = 0
        topic_0_merged_text = merge_tweet_text_for_topic(tweets_and_topics_probs, topic_index)
        len_topic_0_merged_text = len(topic_0_merged_text)
        if len_topic_0_merged_text > 12000:
            batches = divide_in_batch(topic_0_merged_text, batch_max_size)
            merged_batches = merge_selected_batches(batches)
            openAI_response = generate_summary(merged_batches)
        else:
            openAI_response = generate_summary(topic_0_merged_text)
    if selected_option2:
        topic_index = 1
        topic_1_merged_text = merge_tweet_text_for_topic(tweets_and_topics_probs, topic_index)
        len_topic_1_merged_text = len(topic_1_merged_text)
        if len_topic_1_merged_text > 12000:
            batches = divide_in_batch(topic_1_merged_text, batch_max_size)
            merged_batches = merge_selected_batches(batches)
            openAI_response = generate_summary(merged_batches)
        else:
            openAI_response = generate_summary(topic_1_merged_text)
    if selected_option3:
        topic_index = 2
        topic_2_merged_text = merge_tweet_text_for_topic(tweets_and_topics_probs, topic_index)
        len_topic_2_merged_text = len(topic_2_merged_text)
        if len_topic_2_merged_text > 12000:
            batches = divide_in_batch(topic_2_merged_text, batch_max_size)
            merged_batches = merge_selected_batches(batches)
            openAI_response = generate_summary(merged_batches)
        else:
            openAI_response = generate_summary(topic_2_merged_text)
    if selected_option4:
        topic_index = 3
        topic_3_merged_text = merge_tweet_text_for_topic(tweets_and_topics_probs, topic_index)
        len_topic_3_merged_text = len(topic_3_merged_text)
        if len_topic_3_merged_text > 12000:
            batches = divide_in_batch(topic_3_merged_text, batch_max_size)
            merged_batches = merge_selected_batches(batches)
            openAI_response = generate_summary(merged_batches)
        else:
            openAI_response = generate_summary(topic_3_merged_text)
    if selected_option5:
        topic_index = 4
        topic_4_merged_text = merge_tweet_text_for_topic(tweets_and_topics_probs, topic_index)
        len_topic_4_merged_text = len(topic_4_merged_text)
        if len_topic_4_merged_text > 12000:
            batches = divide_in_batch(topic_4_merged_text, batch_max_size)
            merged_batches = merge_selected_batches(batches)
            openAI_response = generate_summary(merged_batches)
        else:
            openAI_response = generate_summary(topic_4_merged_text)

st.text_area('Summary', value=openAI_response, height=300, label_visibility="hidden", placeholder="Seleziona uno dei 5 topic disponibili per generare un summary dei tweet scritti dall'utente relativi al topic selezionato...")
