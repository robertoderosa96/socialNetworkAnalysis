import streamlit as st
from Neo4j import Neo4jConnection
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px

st.set_page_config(page_title='SSCN - Sentiment Analytics', layout='wide', page_icon='📊')

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

st.markdown("<h3 style='text-align: center; font-weight: bold;'>Seleziona un topic per visualizzare il sentiment dell'utente</h3>", unsafe_allow_html=True)

col_sa_button_1, col_sa_button_2, col_sa_button_3, col_sa_button_4, col_sa_button_5 = st.columns(5)

with col_sa_button_1:
    selected_sa_option1 = st.button("Presidenza di Trump", key="topic 0", use_container_width=True)
with col_sa_button_2:
    selected_sa_option2 = st.button("Campagna Presidenziale", key="topic 1", use_container_width=True)
with col_sa_button_3:
    selected_sa_option3 = st.button("Confronto elettorale", key="topic 2", use_container_width=True)
with col_sa_button_4:
    selected_sa_option4 = st.button("Impatto delle Elezioni", key="topic 3", use_container_width=True)
with col_sa_button_5:
    selected_sa_option5 = st.button('Caso Georgia', key="topic 4", use_container_width=True)

st.divider()

def create_sentiment_dataframe(topic_sentiment_scores):
    topic_sentiment_scores = pd.DataFrame(topic_sentiment_scores, columns=['date', 'sa_positive_score', 'sa_neutral_score', 'sa_negative_score'])
    topic_sentiment_scores['date'] = pd.to_datetime(topic_sentiment_scores['date'])
    topic_sentiment_scores = topic_sentiment_scores.sort_values(by=['date'])
    return topic_sentiment_scores

line_colors = ['#0BDA51', '#6495ED', '#D22B2B']
sentiment_bar_charts_labels = ["Positivo", "Neutrale", "Negativo"]
sentiment_col_line = st.columns([1])[0]
sentiment_col_bar, sentiment_col_radar = st.columns([6, 4])

def create_sentiment_line_chart(topic_sentiment_scored_df):
    topic_sentiment_line_chart = go.Figure()
    topic_sentiment_line_chart.add_trace(go.Scatter(x=topic_sentiment_scored_df['date'], y=topic_sentiment_scored_df['sa_positive_score'], name='Positivo', line=dict(color='#0BDA51')))
    topic_sentiment_line_chart.add_trace(go.Scatter(x=topic_sentiment_scored_df['date'], y=topic_sentiment_scored_df['sa_neutral_score'], name='Neutrale', line=dict(color='#6495ED')))
    topic_sentiment_line_chart.add_trace(go.Scatter(x=topic_sentiment_scored_df['date'], y=topic_sentiment_scored_df['sa_negative_score'], name='Negativo', line=dict(color='#D22B2B')))
    topic_sentiment_line_chart.update_layout(title="Evoluzione del sentiment", title_x=0.4, yaxis=dict(range=[0, 1]))
    topic_sentiment_line_chart.update_xaxes(title="Data")
    topic_sentiment_line_chart.update_yaxes(title="Sentiment score")
    topic_sentiment_line_chart.update_layout(legend=dict(title="Sentiment"))
    topic_sentiment_line_chart.add_shape(type="line", x0=topic_sentiment_scored_df['date'].min(), x1=topic_sentiment_scored_df['date'].max(), y0=0.5, y1=0.5, line=dict(color='#818589', width=2, dash='dash'))
    with sentiment_col_line:
        st.plotly_chart(topic_sentiment_line_chart, use_container_width=True)
        st.divider()


def create_sentiment_bar_chart(topic_sentiment_scored_df):
    scores_count = topic_sentiment_scored_df.shape[0]

    topic_positive_score_sum = topic_sentiment_scored_df['sa_positive_score'].sum()
    topic_neutral_score_sum = topic_sentiment_scored_df['sa_neutral_score'].sum()
    topic_negative_score_sum = topic_sentiment_scored_df['sa_negative_score'].sum()

    topic_avg_positive_score = round(topic_positive_score_sum / scores_count * 100, 2)
    topic_avg_neutral_score = round(topic_neutral_score_sum / scores_count * 100, 2)
    topic_avg_negative_score = round(topic_negative_score_sum / scores_count * 100, 2)

    topic_sentiment_scores_list = [topic_avg_positive_score, topic_avg_neutral_score, topic_avg_negative_score]

    topic_sentiment_percent_chart = go.Figure(go.Bar(x=topic_sentiment_scores_list, y=sentiment_bar_charts_labels, orientation='h', marker=dict(color=line_colors)))
    topic_sentiment_percent_chart.update_xaxes(title="% Topic Sentiment Score")
    topic_sentiment_percent_chart.update_yaxes(title="Sentiment")
    topic_sentiment_percent_chart.update_layout(title="Distribuzione percentuale del sentiment", title_x=0.4)
    with sentiment_col_bar:
        st.plotly_chart(topic_sentiment_percent_chart, use_container_width=True)

def create_radar_chart(topic_sentiment_scored_df):
    topic_positive_score_sum = topic_sentiment_scored_df['sa_positive_score'].sum()
    topic_neutral_score_sum = topic_sentiment_scored_df['sa_neutral_score'].sum()
    topic_negative_score_sum = topic_sentiment_scored_df['sa_negative_score'].sum()

    scores_count = topic_sentiment_scored_df.shape[0]

    topic_avg_positive_score = round(topic_positive_score_sum / scores_count, 2)
    topic_avg_neutral_score = round(topic_neutral_score_sum / scores_count, 2)
    topic_avg_negative_score = round(topic_negative_score_sum / scores_count, 2)

    df = pd.DataFrame(dict(r=[topic_avg_positive_score, topic_avg_neutral_score, topic_avg_negative_score], theta=['Positivo', 'Neutrale', 'Negativo']))
    topic_sentiment_radar_chart = px.line_polar(df, r='r', theta='theta', line_close=True)
    topic_sentiment_radar_chart.update_traces(fill='toself')
    topic_sentiment_radar_chart.update_layout(title="Sentiment medio", title_x=0.4, polar=dict(radialaxis=dict(range=[0, 1])))
    topic_sentiment_radar_chart.update_polars(radialaxis_gridwidth=2, gridshape='linear',bgcolor="rgba(0, 0, 0, 0.1)")
    with sentiment_col_radar:
        st.plotly_chart(topic_sentiment_radar_chart, use_container_width=True)

if selected_sa_option1:
    query_get_topic_0_sentiment_scores = 'MATCH (user:User {screen_name: $screen_name}) ' \
                                         'MATCH (user)-[:POSTS]->(tweet:Tweet) ' \
                                         'WHERE tweet.topic_0_prob > tweet.topic_1_prob ' \
                                         'AND tweet.topic_0_prob > tweet.topic_2_prob ' \
                                         'AND tweet.topic_0_prob > tweet.topic_3_prob ' \
                                         'AND tweet.topic_0_prob > tweet.topic_4_prob ' \
                                         'RETURN tweet.date AS date, ' \
                                         'tweet.sa_positive_score AS sa_positive_score, ' \
                                         'tweet.sa_neutral_score AS sa_neutral__score, ' \
                                         'tweet.sa_negative_score AS sa_negative_score'
    topic_0_sentiment_scores = st.session_state.conn.query(query_get_topic_0_sentiment_scores, selected_screen_name)
    topic_0_sentiment_scored_df = create_sentiment_dataframe(topic_0_sentiment_scores)
    create_sentiment_line_chart(topic_0_sentiment_scored_df)
    create_sentiment_bar_chart(topic_0_sentiment_scored_df)
    create_radar_chart(topic_0_sentiment_scored_df)

if selected_sa_option2:
    query_get_topic_1_sentiment_scores = 'MATCH (user:User {screen_name: $screen_name}) ' \
                                         'MATCH (user)-[:POSTS]->(tweet:Tweet) ' \
                                         'WHERE tweet.topic_1_prob > tweet.topic_0_prob ' \
                                         'AND tweet.topic_1_prob > tweet.topic_2_prob ' \
                                         'AND tweet.topic_1_prob > tweet.topic_3_prob ' \
                                         'AND tweet.topic_1_prob > tweet.topic_4_prob ' \
                                         'RETURN tweet.date AS date, ' \
                                         'tweet.sa_positive_score AS sa_positive_score, ' \
                                         'tweet.sa_neutral_score AS sa_neutral__score, ' \
                                         'tweet.sa_negative_score AS sa_negative_score'
    topic_1_sentiment_scores = st.session_state.conn.query(query_get_topic_1_sentiment_scores, selected_screen_name)
    topic_1_sentiment_scored_df = create_sentiment_dataframe(topic_1_sentiment_scores)
    create_sentiment_line_chart(topic_1_sentiment_scored_df)
    create_sentiment_bar_chart(topic_1_sentiment_scored_df)
    create_radar_chart(topic_1_sentiment_scored_df)

if selected_sa_option3:
    query_get_topic_2_sentiment_scores = 'MATCH (user:User {screen_name: $screen_name}) ' \
                                         'MATCH (user)-[:POSTS]->(tweet:Tweet) ' \
                                         'WHERE tweet.topic_2_prob > tweet.topic_0_prob ' \
                                         'AND tweet.topic_2_prob > tweet.topic_1_prob ' \
                                         'AND tweet.topic_2_prob > tweet.topic_3_prob ' \
                                         'AND tweet.topic_2_prob > tweet.topic_4_prob ' \
                                         'RETURN tweet.date AS date, ' \
                                         'tweet.sa_positive_score AS sa_positive_score, ' \
                                         'tweet.sa_neutral_score AS sa_neutral__score, ' \
                                         'tweet.sa_negative_score AS sa_negative_score'
    topic_2_sentiment_scores = st.session_state.conn.query(query_get_topic_2_sentiment_scores, selected_screen_name)
    topic_2_sentiment_scored_df = create_sentiment_dataframe(topic_2_sentiment_scores)
    create_sentiment_line_chart(topic_2_sentiment_scored_df)
    create_sentiment_bar_chart(topic_2_sentiment_scored_df)
    create_radar_chart(topic_2_sentiment_scored_df)

if selected_sa_option4:
    query_get_topic_3_sentiment_scores = 'MATCH (user:User {screen_name: $screen_name}) ' \
                                         'MATCH (user)-[:POSTS]->(tweet:Tweet) ' \
                                         'WHERE tweet.topic_2_prob > tweet.topic_0_prob ' \
                                         'AND tweet.topic_3_prob > tweet.topic_1_prob ' \
                                         'AND tweet.topic_3_prob > tweet.topic_2_prob ' \
                                         'AND tweet.topic_3_prob > tweet.topic_4_prob ' \
                                         'RETURN tweet.date AS date, ' \
                                         'tweet.sa_positive_score AS sa_positive_score, ' \
                                         'tweet.sa_neutral_score AS sa_neutral__score, ' \
                                         'tweet.sa_negative_score AS sa_negative_score'
    topic_3_sentiment_scores = st.session_state.conn.query(query_get_topic_3_sentiment_scores, selected_screen_name)
    topic_3_sentiment_scored_df = create_sentiment_dataframe(topic_3_sentiment_scores)
    create_sentiment_line_chart(topic_3_sentiment_scored_df)
    create_sentiment_bar_chart(topic_3_sentiment_scored_df)
    create_radar_chart(topic_3_sentiment_scored_df)

if selected_sa_option5:
    query_get_topic_4_sentiment_scores = 'MATCH (user:User {screen_name: $screen_name}) ' \
                                         'MATCH (user)-[:POSTS]->(tweet:Tweet) ' \
                                         'WHERE tweet.topic_4_prob > tweet.topic_0_prob ' \
                                         'AND tweet.topic_4_prob > tweet.topic_1_prob ' \
                                         'AND tweet.topic_4_prob > tweet.topic_2_prob ' \
                                         'AND tweet.topic_4_prob > tweet.topic_3_prob ' \
                                         'RETURN tweet.date AS date, ' \
                                         'tweet.sa_positive_score AS sa_positive_score, ' \
                                         'tweet.sa_neutral_score AS sa_neutral__score, ' \
                                         'tweet.sa_negative_score AS sa_negative_score'
    topic_4_sentiment_scores = st.session_state.conn.query(query_get_topic_4_sentiment_scores, selected_screen_name)
    topic_4_sentiment_scored_df = create_sentiment_dataframe(topic_4_sentiment_scores)
    create_sentiment_line_chart(topic_4_sentiment_scored_df)
    create_sentiment_bar_chart(topic_4_sentiment_scored_df)
    create_radar_chart(topic_4_sentiment_scored_df)