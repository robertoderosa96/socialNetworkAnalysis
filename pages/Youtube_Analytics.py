import streamlit as st
from Neo4j import Neo4jConnection
import plotly.graph_objects as go
import pandas as pd
import urllib.parse as urlparse
import tldextract
from youtube_api import YouTubeDataAPI
import tqdm
from datetime import datetime
import openai
import re
import random

openai.api_key = "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"

st.set_page_config(page_title='SSCN - Youtube Analytics', layout='wide', page_icon='📊')

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
line_colors = ['#0BDA51', '#6495ED', '#D22B2B']

def extract_top_domain(url):
    """ Function to extract the top-level domain of a given URL """
    tsd, td, tsu = tldextract.extract(url)
    domain = td + '.' + tsu
    return domain.lower()

def url2id(yt_url):
    """
    Function to extract the YT video id from its url

    Supported formats:
    - http://youtu.be/SA2iWivDJiE
    - http://www.youtube.com/watch?v=_oPAwA_Udwc&feature=feedu
    - http://www.youtube.com/embed/SA2iWivDJiE
    - http://www.youtube.com/v/SA2iWivDJiE?version=3&amp;hl=en_US
    """
    query = urlparse.urlparse(yt_url)
    if query.hostname == 'youtu.be':
        return query.path[1:]
    if query.hostname in ('www.youtube.com', 'youtube.com'):
        if query.path == '/watch':
            p = urlparse.parse_qs(query.query)
            return p.get('v', [None])[0]
        if query.path[:7] == '/embed/':
            return query.path.split('/')[2]
        if query.path[:8] == '/shorts/':
            return query.path.split('/')[2]
        if query.path[:3] == '/v/':
            return query.path.split('/')[2]
    # fail?
    return None

def get_video_metadata(ids, API_KEY, verbose=False):
    """
    Function to extract metadata (e.g., video title, # of likes) for a list of YT videos (represented through their ids).

    Parameters:
    - ids -> list of YT video ids
    - API_KEY -> YouTube API developer key

    Return:
    - df_video_metadata -> pandas dataframe

    N.B.:
    If a YT video id is not mapped to any metadata, then that video does not exist on YT anymore.
    """

    def batch(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    def preprocess_video_metadata(df_video_metadata):

        df_video_metadata['video_publish_date'] = df_video_metadata['video_publish_date'].apply(
            lambda timestamp: datetime.fromtimestamp(timestamp))
        df_video_metadata['video_category'] = df_video_metadata['video_category'].astype(float)
        df_video_metadata['video_view_count'] = df_video_metadata['video_view_count'].astype(float)
        df_video_metadata['video_comment_count'] = df_video_metadata['video_comment_count'].astype(float)
        df_video_metadata['video_like_count'] = df_video_metadata['video_like_count'].astype(float)
        df_video_metadata['video_dislike_count'] = df_video_metadata['video_dislike_count'].astype(float)
        df_video_metadata['video_tags'] = df_video_metadata['video_tags'].apply(lambda x: x.split('|'))

        return df_video_metadata

    results, problematic_ids = [], []

    yt_handler = YouTubeDataAPI(API_KEY)

    try:
        if verbose:
            for btc in tqdm(batch(ids, n=50)):
                results.append(yt_handler.get_video_metadata(btc, part=['statistics', 'snippet']))
        else:
            for btc in batch(ids, n=50):
                results.append(yt_handler.get_video_metadata(btc, part=['statistics', 'snippet']))
    except Exception:
        problematic_ids.append(btc)

    results = [item for sublist in results for item in sublist]
    problematic_ids = [item for sublist in problematic_ids for item in sublist]

    for problematic_id in problematic_ids:
        try:
            metadata = yt_handler.get_video_metadata(problematic_id, part=['statistics', 'snippet'])
            if len(metadata) != 0:
                results.append(metadata)
        except:
            pass

    df_video_metadata = pd.DataFrame.from_records(results)
    df_video_metadata = preprocess_video_metadata(df_video_metadata)

    return df_video_metadata

API_KEY="AIzaSyA-J59BATqWBUe2kAz3J-Dy6-BUesK51DA"

query_get_user_cat = 'MATCH (user:User {screen_name: $screen_name}) WHERE user.cat IS NOT NULL RETURN user.cat AS cat'
user_cat_result = st.session_state.conn.query(query_get_user_cat, selected_screen_name)

user_cat=""
for record in user_cat_result:
    user_cat = record['cat']

query_get_user_yt_videos_url = 'MATCH (user:User {screen_name: $screen_name})-[:POSTS]->(tweet:Tweet)-[:CONTAINS]->(link:Link) '\
                               'WHERE link.name '\
                               'CONTAINS "http://youtu.be/" OR '\
                               'link.name CONTAINS "http://www.youtube.com/watch?v" OR '\
                               'link.name CONTAINS "https://www.youtube.com/watch?v" OR '\
                               'link.name CONTAINS "https://youtu.be/" '\
                               'RETURN user.cat AS cat, '\
                               'tweet.date AS sharing_date, '\
                               'tweet.sa_positive_score AS sa_positive_score, '\
                               'tweet.sa_neutral_score AS sa_neutral_score, '\
                               'tweet.sa_negative_score AS sa_negative_score, '\
                               'tweet.tweet_type AS tweet_type, '\
                               'tweet.text AS tweet_text, '\
                               'link.name AS video_url '\
                               'ORDER BY sharing_date ASC'
user_tweet_data_with_yt_video = st.session_state.conn.query(query_get_user_yt_videos_url, selected_screen_name)
shared_videos_number = len(user_tweet_data_with_yt_video)

if (shared_videos_number==0):
    st.write("L' utente non ha condiviso alcun video")
else:

    if (user_cat == "NMYT"):
        st.markdown(
            """
            <style>
            .styled-row {
                background-color: #50C878;
                border-radius: 10px;
                padding: 5px;
                margin-bottom: 10px;
                display: flex;
                height: 100px;
                justify-content: center;
                align-items: center;
                text-align: center;
                font-size: 22px;
                font-weight: bold;
                text-size: 24pc
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        st.markdown('<div class="styled-row">L\'utente non ha mai condiviso video moderati</div>', unsafe_allow_html=True)
    elif (user_cat=="MYT"):
        st.markdown(
            """
            <style>
            .styled-row {
                background-color: #FFAC1C;
                border-radius: 10px;
                padding: 5px;
                margin-bottom: 10px;
                display: flex;
                height: 100px;
                justify-content: center;
                align-items: center;
                text-align: center;
                font-size: 22px;
                font-weight: bold;
                text-size: 24pc
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        st.markdown('<div class="styled-row">L\'utente ha condiviso video moderati</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            """
            <style>
            .styled-row {
                background-color: #DC143C;
                border-radius: 10px;
                padding: 5px;
                margin-bottom: 10px;
                display: flex;
                height: 100px;
                justify-content: center;
                align-items: center;
                text-align: center;
                font-size: 22px;
                font-weight: bold;
                text-size: 24pc
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        st.markdown('<div class="styled-row">Errore: Non è stato possibile ricavare la categoria dell\' utente</div>', unsafe_allow_html=True)

    tweets_with_video_data_columns = ['tweet_sharing_date', 'tweet_text', 'sa_positive_score', 'sa_neutral_score', 'sa_negative_score', 'video_id']
    tweets_with_video_data_df = pd.DataFrame(columns=tweets_with_video_data_columns)

    yt_videos_ids_list = []
    count_record = 0
    for record in user_tweet_data_with_yt_video:
        tweet_sharing_date = record['sharing_date']
        sa_positive_score = record['sa_positive_score']
        sa_neutral_score = record['sa_neutral_score']
        sa_negative_score = record['sa_negative_score']
        tweet_text = record['tweet_text']

        video_url = record['video_url']
        video_id = url2id(video_url)
        yt_videos_ids_list.append(video_id)

        tweets_with_video_data_df.loc[count_record] = [tweet_sharing_date, tweet_text, sa_positive_score, sa_neutral_score, sa_negative_score, video_id]
        count_record = count_record + 1

    yt_videos_ids_distinct_list = list(set(yt_videos_ids_list))
    shared_distinct_videos_number = len(yt_videos_ids_distinct_list)

    videos_metadata_df = get_video_metadata(yt_videos_ids_distinct_list, API_KEY, verbose=False)
    non_moderated_videos_number = videos_metadata_df.shape[0]

    moderated_videos_number = shared_distinct_videos_number - non_moderated_videos_number

    yt_metrics_style = st.markdown(
        """
        <style>
        #shared_video_number_metric, 
        #mod_shared_video_number_metric,
        #views_number_metric,
        #comments_number_metric,
        #like_number_metric
        {
            background-color: #ffffff00;
            border-radius: 10px;
            padding: 5px;
            margin-bottom: 10px;
            display: flex;
            height: 100px;
            justify-content: center;
            align-items: center;
            text-align: center;
            font-size: 22px;
            text-size: 24pc;
            flex-direction: column !important;
            font-weight: normal
        }
        </style>
        """,
        unsafe_allow_html=True)

    st.divider()

    yt_metric_col1, yt_metric_col2 = st.columns(2)
    with yt_metric_col1:
        st.markdown('<div id="shared_video_number_metric" class="styled-row">Totale video condivisi <p style="font-size: 32px; font-weight: bold">'+str(shared_distinct_videos_number)+'</p></div>', unsafe_allow_html=True)
    with yt_metric_col2:
        st.markdown('<div id="mod_shared_video_number_metric" class="styled-row">Totale video moderati condivisi <p style="font-size: 32px; font-weight: bold">'+str(moderated_videos_number)+'</p></div>', unsafe_allow_html=True)

    def generate_mod_video_tweet__summary(merged_text):
        prompt = "Mi puoi generare un summary, in italiano, di questi tweet evidenziando il pensiero e il sentiment dell'utente"
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=1,
            max_tokens=500,
            top_p=1.0,
            frequency_penalty=0.5,
            presence_penalty=0.0,
            messages=[
                {
                    "role": "user",
                    "content": f"{prompt} Il testo da esaminare è il seguente: {merged_text}",
                },
            ],
        )
        summary_string_response = response["choices"][0]["message"]["content"]
        return summary_string_response

    st.divider()

    if moderated_videos_number>0:
        yt_spinner = st.spinner("Generazione del summary...")

        mod_openAI_response = ""

        st.write("Cliccare il pulsante per generare un summary dei tweet dell'utente in cui viene condiviso un video YouTube moderato")

        button_yt_summary = st.button("Genera summary", key="button_summary_yt")

        with yt_spinner:
            if button_yt_summary:
                tweet_moderated_videos_list = []
                moderated_videos_data = tweets_with_video_data_df[~tweets_with_video_data_df['video_id'].isin(videos_metadata_df['video_id'])]
                for key,row in moderated_videos_data.iterrows():
                    tweet_moderated_videos_list.append(row['tweet_text'])

                delimiter = '\n'
                merged_moderated_video_tweet_text = delimiter.join(tweet_moderated_videos_list)
                merged_moderated_video_tweet_text = remove_links(merged_moderated_video_tweet_text)
                merged_moderated_video_tweet_text = remove_rt(merged_moderated_video_tweet_text)
                merged_moderated_video_tweet_text = remove_emoji(merged_moderated_video_tweet_text)

                #ADESSO DEVO FARE IL FATTO DEI BATCH PER CHIAMARE CHATGPT PER IL SUMMARY
                #DEVO CHIARARE UNA NUOVA API OPENAI PERHCÈ IL PROMPT CAMBIA!!!!!

                len_merged_moderated_video_tweet_text = len(merged_moderated_video_tweet_text)
                if len_merged_moderated_video_tweet_text > 12000:
                    mod_tweet_text_batches = divide_in_batch(merged_moderated_video_tweet_text, batch_max_size)
                    mod_merged_batches = merge_selected_batches(mod_tweet_text_batches)
                    mod_openAI_response = generate_mod_video_tweet__summary(mod_merged_batches)
                else:
                    mod_openAI_response = generate_mod_video_tweet__summary(merged_moderated_video_tweet_text)

                #TEXT AREA CON SUMMARY
                st.text_area('Summary dei tweet dell\'utente in cui viene condiviso un video YouTube moderato', value=mod_openAI_response, label_visibility="hidden", height=150, placeholder="Seleziona uno dei 5 topic disponibili per generare un summary dei tweet scritti dall'utente relativi al topic selezionato...")

    merged_videos_and_tweets_data_df = pd.merge(videos_metadata_df, tweets_with_video_data_df, on='video_id', how='inner')
    merged_videos_and_tweets_data_df['tweet_sharing_date'] = pd.to_datetime(merged_videos_and_tweets_data_df['tweet_sharing_date'])

    first_tweet_with_video_sharing_date = merged_videos_and_tweets_data_df['tweet_sharing_date'].min().to_pydatetime()
    last_tweet_with_video_sharing_date = merged_videos_and_tweets_data_df['tweet_sharing_date'].max().to_pydatetime()

    st.divider()

    selected_date = st.slider('Seleziona una data iniziale ed una data finale:', min_value=first_tweet_with_video_sharing_date, max_value=last_tweet_with_video_sharing_date,
                                 value=(first_tweet_with_video_sharing_date, first_tweet_with_video_sharing_date), format="DD/MM/YYYY")

    st.divider()

    filtered_merged_videos_and_tweets_data_df = merged_videos_and_tweets_data_df[(merged_videos_and_tweets_data_df['tweet_sharing_date'] >= selected_date[0]) & (merged_videos_and_tweets_data_df['tweet_sharing_date'] <= selected_date[1])]

    for index, row in filtered_merged_videos_and_tweets_data_df.iterrows():
        container = st.container()
        unique_key_description_text_area = f"text_area_{index}"
        with container:
            metadata_col, sentiment_col = st.columns([6, 4])
            with metadata_col:
                video_title = row['video_title']
                channel_title = row['channel_title']
                video_view_count = str(int(row['video_view_count']))
                video_comment_count = str(int(row['video_comment_count']))
                video_like_count = str(int(row['video_like_count']))
                sharing_date = row['tweet_sharing_date']

                st.markdown("<p>Titolo: <b>"+str(video_title)+"</b></p>",unsafe_allow_html=True)

                st.divider()

                yt_metric_col3, yt_metric_col4, yt_metric_col5 = st.columns(3)
                with yt_metric_col3:
                    st.markdown(
                        '<div id="views_number_metric" class="styled-row">Visualizzazioni <p style="font-size: 32px; font-weight: bold">' + str(
                            video_view_count) + '</p></div>', unsafe_allow_html=True)

                with yt_metric_col4:
                    st.markdown(
                        '<div id="comments_number_metric" class="styled-row">Commenti <p style="font-size: 32px; font-weight: bold">' + str(
                            video_comment_count) + '</p></div>', unsafe_allow_html=True)

                with yt_metric_col5:
                    st.markdown(
                        '<div id="like_number_metric" class="styled-row">Likes <p style="font-size: 32px; font-weight: bold">' + str(
                            video_like_count) + '</p></div>', unsafe_allow_html=True)

                st.divider()

                st.markdown("<p>Nome del canale: <b>"+str(channel_title)+"</b></p>", unsafe_allow_html=True)
                st.markdown("<p>Data di condivisione su Twitter: <b>"+str(sharing_date)+"</b></p>" , unsafe_allow_html=True)

                video_description = row['video_description']
                st.text_area("Descrizione video", value=video_description, height=200, key=unique_key_description_text_area)

            with sentiment_col:
                tweet_scores_list = [row['sa_positive_score'], row['sa_neutral_score'], row['sa_negative_score']]
                sentiment_labels = ['Positive', 'Neutral', 'Negative']
                tweet_sentiment_chart = go.Figure(go.Bar(x=sentiment_labels, y=tweet_scores_list, marker=dict(color=line_colors)))
                tweet_sentiment_chart.update_yaxes(title="Tweet Sentiment Score")
                tweet_sentiment_chart.update_xaxes(title="Sentiment")
                tweet_sentiment_chart.update_layout(title="Score sentiment del tweet in cui è stato ricondiviso il video")
                st.plotly_chart(tweet_sentiment_chart, use_container_width=True, height="100%")

            st.divider()