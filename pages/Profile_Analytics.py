import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from Neo4j import Neo4jConnection

st.set_page_config(page_title='SSCN - Profile Analyitcs', layout='wide', page_icon='📊')

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

# <-- Grafo followers count -->
query_followers_count = 'MATCH (user:User {screen_name: $screen_name})--(tweet:Tweet) RETURN tweet.date AS data, tweet.followers_count AS followers_count'
followers_count = st.session_state.conn.query(query_followers_count, selected_screen_name)
df_followers_count = pd.DataFrame(followers_count, columns=['data', 'followers_count'])
df_followers_count['data'] = pd.to_datetime(df_followers_count['data'])
df_followers_count = df_followers_count.sort_values(by=['data'])

figFollowersCount = go.Figure()
figFollowersCount.update_layout(title_text="Andamento numero di follower")

# <-- /Grafo followers count -->

# <-- Grafo favourites_count -->
query_favourites_count = 'MATCH (user:User {screen_name: $screen_name})--(tweet:Tweet) RETURN tweet.date AS data, tweet.favourites_count AS favourites_count'
favourites_count = st.session_state.conn.query(query_favourites_count, selected_screen_name)
df_favourites_count = pd.DataFrame(favourites_count, columns=['data', 'favourites_count'])
df_favourites_count['data'] = pd.to_datetime(df_favourites_count['data'])
df_favourites_count = df_favourites_count.sort_values(by=['data'])

figFavouritesCount = go.Figure()
figFavouritesCount.update_layout(title_text="Andamento numero di like")

# <-- /Grafo favourites_count -->

# <-- Grafo friends_count -->
query_friends_count = 'MATCH (user:User {screen_name: $screen_name})--(tweet:Tweet) RETURN tweet.date AS data, tweet.friends_count AS friends_count'
friends_count = st.session_state.conn.query(query_friends_count, selected_screen_name)
df_friends_count = pd.DataFrame(friends_count, columns=['data', 'friends_count'])
df_friends_count['data'] = pd.to_datetime(df_friends_count['data'])
df_friends_count = df_friends_count.sort_values(by=['data'])

figFriendsCount = go.Figure()
figFriendsCount.update_layout(title_text="Andamento numero di seguiti")

# <-- /Grafo friends_count -->

# Metric like
initial_favourite_number = df_favourites_count['favourites_count'].iloc[0]
final_favourite_number = df_favourites_count['favourites_count'].iloc[-1]
delta_favourite_number = final_favourite_number - initial_favourite_number

# Metric follower
initial_follower_number = df_followers_count['followers_count'].iloc[0]
final_follower_number = df_followers_count['followers_count'].iloc[-1]
delta_follower_number = final_follower_number - initial_follower_number

# Metric friends
initial_friend_number = df_friends_count['friends_count'].iloc[0]
final_friend_number = df_friends_count['friends_count'].iloc[-1]
delta_friend_number = final_friend_number - initial_friend_number

colProfileTab1, colProfileTab2, colProfileTab3 = st.columns(3)

with st.container():
    colProfileTab1.metric("Numero di follower", final_follower_number, int(delta_follower_number))

    colProfileTab2.metric("Numero di seguiti", final_friend_number, int(delta_friend_number))

    colProfileTab3.metric("Numero di like", final_favourite_number, int(delta_favourite_number))

st.divider()

with st.container():

    data_min_followers_count = df_followers_count['data'].min().to_pydatetime()
    data_max_followers_count = df_followers_count['data'].max().to_pydatetime()

    selected_date_followers_count = st.slider('Seleziona una data iniziale ed una data finale:',
                              min_value=data_min_followers_count,
                              max_value=data_max_followers_count,
                              value=(data_min_followers_count, data_max_followers_count),
                              format="DD/MM/YYYY",
                              key="selected_date_followers_count")

    filtered_followers_count = df_followers_count[(df_followers_count['data'] >= selected_date_followers_count[0]) & (df_followers_count['data'] <= selected_date_followers_count[1])]

    figFollowersCount.add_trace(go.Scatter(x=filtered_followers_count['data'], y=filtered_followers_count['followers_count'], name='Numero di follower'))

    st.plotly_chart(figFollowersCount, use_container_width=True)

st.divider()

with st.container():
    data_min_friends_count = df_friends_count['data'].min().to_pydatetime()
    data_max_friends_count = df_friends_count['data'].max().to_pydatetime()

    selected_date_friends_count = st.slider('Seleziona una data iniziale ed una data finale:',
                              min_value=data_min_friends_count,
                              max_value=data_max_friends_count,
                              value=(data_min_friends_count, data_max_friends_count),
                              format="DD/MM/YYYY",
                              key="selected_date_friends_count")

    filtered_friends_count = df_friends_count[(df_friends_count['data'] >= selected_date_friends_count[0]) & (df_friends_count['data'] <= selected_date_friends_count[1])]

    figFriendsCount.add_trace(go.Scatter(x=filtered_friends_count['data'], y=filtered_friends_count['friends_count'], name='Numero di seguiti'))

    st.plotly_chart(figFriendsCount, use_container_width=True)

st.divider()

with st.container():
    data_min_favourites_count = df_favourites_count['data'].min().to_pydatetime()
    data_max_favourites_count = df_favourites_count['data'].max().to_pydatetime()

    selected_date_favourites_count = st.slider('Seleziona una data iniziale ed una data finale:',
                                            min_value=data_min_favourites_count,
                                            max_value=data_max_favourites_count,
                                            value=(data_min_favourites_count, data_max_favourites_count),
                                            format="DD/MM/YYYY",
                                            key="selected_date_favourites_count")

    filtered_favourites_count = df_favourites_count[(df_favourites_count['data'] >= selected_date_favourites_count[0]) & (df_favourites_count['data'] <= selected_date_favourites_count[1])]

    figFavouritesCount.add_trace(go.Scatter(x=filtered_favourites_count['data'], y=filtered_favourites_count['favourites_count'], name='Numero di like'))

    st.plotly_chart(figFavouritesCount, use_container_width=True)