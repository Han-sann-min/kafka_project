from kafka import KafkaConsumer
import json
import random
import pandas as pd

# Load TMDb dataset
tmdb_dataset = pd.read_csv('output_result.csv')
movie_catalog = {}

# Preprocess data and create a movie catalog
for index, row in tmdb_dataset.iterrows():
    movie_catalog[row['original_title']] = row['genre_names'].split(",") + row['keywords'].split(",")

consumer = KafkaConsumer('movie_recommend2', auto_offset_reset='earliest', group_id='movie_group', value_deserializer=lambda v: json.loads(v.decode('utf-8')))

def generate_recommendation(user_id, movies_watched):
    genre_counts = {genre: sum(1 for movie, movie_genres in movie_catalog.items() if movie in movies_watched and genre in movie_genres) for genre in set(genre for genres in movie_catalog.values() for genre in genres)}
    most_watched_genre = max(genre_counts, key=genre_counts.get)
    
    # Correct way to get genre information
    most_watched_genre_info = {'name': most_watched_genre}

    unwatched_movies = [movie for movie in movie_catalog if movie not in movies_watched]
    recommendation = {'user_id': user_id, 'most_watched_genre': most_watched_genre_info, 'unwatched_movies': random.sample(unwatched_movies, min(3, len(unwatched_movies)))}
    print(f"Recommendation for user {user_id}: {recommendation}")

# Process events
try:
    for message in consumer:
        user_record = message.value
        generate_recommendation(user_record['user_id'], user_record['movies_watched'])

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
