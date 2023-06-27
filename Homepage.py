import streamlit as st
from Neo4j import Neo4jConnection

st.set_page_config(page_title='SSCN - Twitter Analyitcs', layout='wide', page_icon='📊')

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

# Get description of user
query_user_description = 'MATCH (user:User {screen_name: $screen_name}) RETURN user.description AS description'
user_description = st.session_state.conn.query(query_user_description, selected_screen_name)
single_user_description = user_description.__getitem__(0)['description']

query_get_tweets_count_for_user = 'MATCH (:User {screen_name: $screen_name})--(tweet:Tweet) RETURN tweet.tweet_type AS tweet_type, COUNT(tweet) AS countForTweetType'
counts_for_tweets_types = st.session_state.conn.query(query_get_tweets_count_for_user, selected_screen_name)

count_tweets_types_dict = {}

for result in counts_for_tweets_types:
    count_tweets_types_dict[result.values()[0]] = result.values()[1]

st.subheader("Bio del profilo")
with st.container():
    st.caption(single_user_description)

st.divider()

st.subheader('Main Analytics')
with st.container():
    col1, col2, col3 = st.columns(3)
    col1.write('')
    if 'original' in count_tweets_types_dict:
        col2.metric(label="# Tweets", value=count_tweets_types_dict['original'])
    else:
        col2.metric(label="# Tweets", value=0)
    col3.write('')

with st.container():
    col1, col2, col3 = st.columns(3)

    if 'reply' in count_tweets_types_dict:
        col1.metric(label="# Tweet Reply", value=count_tweets_types_dict['reply'])
    else:
        col1.metric(label="# Tweet Reply", value=0)

    if 'quoted_tweet' in count_tweets_types_dict:
        col2.metric(label="# Quoted Tweet", value=count_tweets_types_dict['quoted_tweet'])
    else:
        col2.metric(label="# Quoted Tweet", value=0)

    if 'retweeted_tweet_without_comment' in count_tweets_types_dict:
        col3.metric(label="# Retweets", value=count_tweets_types_dict['retweeted_tweet_without_comment'])
    else:
        col3.metric(label="# Retweets", value=0)