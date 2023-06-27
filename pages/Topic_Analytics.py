import streamlit as st
from Neo4j import Neo4jConnection
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(page_title='SSCN - Topic Analytics', layout='wide', page_icon='📊')

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

# <-- TOPIC -->
average_topics_probs_dict = {}
query_average_topics_probs = 'MATCH (user:User {screen_name: $screen_name})--(tweet:Tweet) ' \
                             'RETURN round(AVG(tweet.topic_0_prob),4) AS topic_0_prob, ' \
                             'round(AVG(tweet.topic_1_prob),4) AS topic_1_prob, ' \
                             'round(AVG(tweet.topic_2_prob),4) AS topic_2_prob, ' \
                             'round(AVG(tweet.topic_3_prob),4) AS topic_3_prob, ' \
                             'round(AVG(tweet.topic_4_prob),4) AS topic_4_prob'
average_topics_probs = st.session_state.conn.query(query_average_topics_probs, selected_screen_name)
print(average_topics_probs)

for result in average_topics_probs:
    average_topics_probs_dict[result.keys()[0]] = result.values()[0]
    average_topics_probs_dict[result.keys()[1]] = result.values()[1]
    average_topics_probs_dict[result.keys()[2]] = result.values()[2]
    average_topics_probs_dict[result.keys()[3]] = result.values()[3]
    average_topics_probs_dict[result.keys()[4]] = result.values()[4]

labels = ['Presidenza di Trump', 'Campagna Presidenziale', 'Confronto elettorale', 'Impatto delle Elezioni', 'Caso Georgia']
values = [average_topics_probs_dict['topic_0_prob'],
            average_topics_probs_dict['topic_1_prob'],
            average_topics_probs_dict['topic_2_prob'],
            average_topics_probs_dict['topic_3_prob'],
            average_topics_probs_dict['topic_4_prob']]

colors = ['#3C99DC', '#66D3FA', '#D5F3FE', '#2565AE', '#0F5298']

fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.5)])
fig.update_traces(hoverinfo='label', textinfo='percent', textfont_size=20,
                    marker=dict(colors=colors, line=dict(color='#000000', width=2)))
fig.update_layout(title_text="Percentuale topic discussi")

# SECONDO GRAFO
query_topics_prob_date = 'MATCH (user:User {screen_name: $screen_name})--(tweet:Tweet) ' \
                             'RETURN round(tweet.topic_0_prob,4) AS topic_0_prob, ' \
                             'round(tweet.topic_1_prob,4) AS topic_1_prob, ' \
                             'round(tweet.topic_2_prob,4) AS topic_2_prob, ' \
                             'round(tweet.topic_3_prob,4) AS topic_3_prob, ' \
                             'round(tweet.topic_4_prob,4) AS topic_4_prob, ' \
                             'tweet.date AS data'
topics_probs_date = st.session_state.conn.query(query_topics_prob_date, selected_screen_name)
df_topics_probs_date = pd.DataFrame(topics_probs_date, columns=['topic_0_prob', 'topic_1_prob', 'topic_2_prob', 'topic_3_prob', 'topic_4_prob', 'data'])
df_topics_probs_date['data'] = pd.to_datetime(df_topics_probs_date['data'])
df_topics_probs_date = df_topics_probs_date.sort_values(by=['data'])

fig2 = go.Figure()
fig2.update_layout(title_text="Andamento topic discussi")

with st.container():
    st.plotly_chart(fig, use_container_width=True)

st.divider()

with st.container():
    data_min = df_topics_probs_date['data'].min().to_pydatetime()
    data_max = df_topics_probs_date['data'].max().to_pydatetime()

    selected_date = st.slider('Seleziona una data iniziale ed una data finale:',
                              min_value=data_min,
                              max_value=data_max,
                              value=(data_min, data_max),
                              format="DD/MM/YYYY")

    filtered_topics_probs_date = df_topics_probs_date[
        (df_topics_probs_date['data'] >= selected_date[0]) & (df_topics_probs_date['data'] <= selected_date[1])]

    fig2.add_trace(go.Scatter(x=filtered_topics_probs_date['data'], y=filtered_topics_probs_date['topic_0_prob'],
                              name='Presidenza di Trump'))
    fig2.add_trace(go.Scatter(x=filtered_topics_probs_date['data'], y=filtered_topics_probs_date['topic_1_prob'],
                              name='Campagna Presidenziale'))
    fig2.add_trace(go.Scatter(x=filtered_topics_probs_date['data'], y=filtered_topics_probs_date['topic_2_prob'],
                              name='Confronto elettorale'))
    fig2.add_trace(go.Scatter(x=filtered_topics_probs_date['data'], y=filtered_topics_probs_date['topic_3_prob'],
                              name='Impatto delle Elezioni'))
    fig2.add_trace(go.Scatter(x=filtered_topics_probs_date['data'], y=filtered_topics_probs_date['topic_4_prob'],
                              name='Caso Georgia'))

    st.plotly_chart(fig2, use_container_width=True)