import pandas as pd
import random
import argparse

# Load TMDb dataset
tmdb_dataset = pd.read_csv('output_result.csv')

def generate_user_records(user_count):
    user_records = []

    for user_id in range(1, user_count + 1):
        # Randomly select 10 movies for each user
        movies_watched = random.sample(list(tmdb_dataset['original_title']), 10)
        
        # Create a user record
        user_record = {'user_id': user_id, 'movies_watched': movies_watched}
        user_records.append(user_record)

    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(user_records)
    df.to_csv('user_records.csv', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate user records with random movie selections.')
    parser.add_argument('user_count', type=int, help='Number of users for whom records should be generated.')

    args = parser.parse_args()

    generate_user_records(args.user_count)
