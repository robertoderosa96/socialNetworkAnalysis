import streamlit as st
import pandas as pd
from Neo4j import Neo4jConnection
import numpy as np
from wordcloud import WordCloud
import plotly.express as px
from PIL import Image

st.set_page_config(page_title='SSCN - Hashtag Analytics', layout='wide', page_icon='📊')

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

# <-- Wordmap -->
with st.spinner('Wait for it...'):
    query_hastag_list = 'MATCH (user:User {screen_name: $screen_name})--(tweet:Tweet)<-[:TAGS]-(hashtag:Hashtag) RETURN hashtag.name AS hashtag, COUNT(hashtag) AS count_hashtag'
    hashtag_list = st.session_state.conn.query(query_hastag_list, selected_screen_name)
    df_hashtag_list = pd.DataFrame(hashtag_list, columns=['hashtag', 'count_hashtag'])
    df_hashtag_list = df_hashtag_list.sort_values(by=['count_hashtag'], ascending=False)
    frequency_hashtag_dict = {}
    for item in hashtag_list:
        frequency_hashtag_dict[item['hashtag']] = int(item['count_hashtag'])

    mask = np.array(Image.open("twitter_logo.png"))
    wordcloud = WordCloud(background_color='#0e1116', mask=mask, width=400, height=200, repeat=True).generate_from_frequencies(frequency_hashtag_dict)
# <-- /Wordmap -->

with st.container():
    colWord1, colWord2, colWord3 = st.columns([1, 3, 1])

    with colWord1:
        st.write('')

    with colWord2:
        st.image(wordcloud.to_image(), width=600)

    with colWord3:
        st.write('')

with st.container():
    figHashtagBar = px.bar(df_hashtag_list.head(20), y='count_hashtag', x='hashtag', title="20 Hashtag più utilizzati").update_layout(xaxis_title="Hashtag", yaxis_title="Count Hashtag")
    figHashtagBar.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
    st.plotly_chart(figHashtagBar, use_container_width=True)