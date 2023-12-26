from kafka import KafkaConsumer
import json
import pandas as pd

# Load TMDb dataset
tmdb_dataset = pd.read_csv('output_result.csv')
movie_catalog = {}

# Preprocess data and create a movie catalog
for index, row in tmdb_dataset.iterrows():
    if not pd.isnull(row['genre_names']):
        genre_names = [genre.strip() for genre in row['genre_names'].split(',')]
        movie_catalog[row['original_title']] = {'genre_names': genre_names, 'popularity': row['popularity']}  # 추가된 부분

consumer = KafkaConsumer('movie_recommend2', auto_offset_reset='earliest', group_id='movie_group', value_deserializer=lambda v: json.loads(v.decode('utf-8')))

def generate_recommendation(user_id, movies_watched, movie_catalog):
    genre_counts = {genre: sum(1 for movie, movie_info in movie_catalog.items() if movie in movies_watched and genre in movie_info['genre_names']) for genre in set(genre for movie_info in movie_catalog.values() for genre in movie_info['genre_names'])}
    most_watched_genre = max(genre_counts, key=genre_counts.get)

    most_watched_genre_info = {'name': most_watched_genre}

    unwatched_movies = [(movie, movie_info) for movie, movie_info in movie_catalog.items() if movie not in movies_watched and most_watched_genre in movie_info['genre_names']]
    
    # Sort unwatched movies by popularity (assuming 'popularity' field exists in the dataset)
    unwatched_movies.sort(key=lambda x: x[1]['popularity'], reverse=True)
    
    # Get top 3 movie titles from the sorted list
    top3_movies = [movie[0] for movie in unwatched_movies[:3]]

    # Display the recommendation
    print(f"User ID: {user_id}, Favorite Genre: {most_watched_genre_info['name']}, Recommended Movies: {', '.join(top3_movies)}")

# Process events
try:
    for message in consumer:
        user_record = message.value
        generate_recommendation(user_record['user_id'], user_record['movies_watched'], movie_catalog)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()