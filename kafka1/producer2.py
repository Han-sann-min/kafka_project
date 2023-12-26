from kafka import KafkaProducer
import pandas as pd
import random
import json
# Load TMDb dataset
tmdb_dataset = pd.read_csv('output_result.csv')
movie_catalog = {}

# Preprocess data and create a movie catalog
for index, row in tmdb_dataset.iterrows():
    # Check if the 'genre_names' column is not empty or null
    if not pd.isnull(row['genre_names']):
        genre_names = [genre.strip() for genre in row['genre_names'].split(',')]
        
        # Combine genre names
        movie_catalog[row['original_title']] = genre_names

# Rest of the code remains the same
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# 사용자 목록
user_records = [
    {'user_id': 1, 'movies_watched': random.sample(list(movie_catalog.keys()), 10)},
    {'user_id': 2, 'movies_watched': random.sample(list(movie_catalog.keys()), 10)},
    # Add more user records as needed
]

for record in user_records:
    producer.send('movie_recommend2', value=record)

producer.flush()