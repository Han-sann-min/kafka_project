import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

# Load TMDb dataset
tmdb_dataset = pd.read_csv('output_result.csv')

# Connect to Kafka consumer
consumer = KafkaConsumer('movie_recommend2', bootstrap_servers='localhost:9092', value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Streamlit app
st.title('Movie Recommendation Streamlit App')

# Sidebar
user_id = st.sidebar.number_input('Enter User ID:', min_value=1)

# Display user information
st.sidebar.markdown(f'**Selected User ID:** {user_id}')

# Main content
st.header('User Movie History')

# Fetch user movies watched from Kafka data
user_movies_watched = []
for message in consumer:
    user_record = message.value
    if user_record['user_id'] == user_id:
        user_movies_watched = user_record['movies_watched']
        break

# Display user movies watched
st.write(f"**User ID: {user_id}**")
st.write(f"**Movies Watched:** {', '.join(user_movies_watched)}")

# Movie Recommendation based on Kafka data
st.header('Movie Recommendations')

# Fetch movie recommendations from Kafka data
movie_recommendations = []
for message in consumer:
    recommendation = message.value
    if 'recommended_movies' in recommendation and recommendation['user_id'] == user_id:
        movie_recommendations = recommendation['recommended_movies']
        break

# Display movie recommendations
st.subheader('Top 3 Recommended Movies:')
for i, movie in enumerate(movie_recommendations[:3], 1):
    st.write(f"{i}. Recommended Movie: {movie}")

# Closing Kafka consumer
consumer.close()
