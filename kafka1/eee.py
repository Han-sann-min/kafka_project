import pandas as pd
import ast

# 데이터셋 로드
tmdb_dataset = pd.read_csv('tmdb_5000_movies.csv')

# genres 컬럼의 값을 리스트로 변환
tmdb_dataset['genres'] = tmdb_dataset['genres'].apply(ast.literal_eval)

# 새로운 컬럼인 genre_names 추가
tmdb_dataset['genre_names'] = tmdb_dataset['genres'].apply(lambda x: ', '.join([genre['name'] for genre in x]) if isinstance(x, list) else '')

# 'keywords' 컬럼을 읽어와서 파이썬 리스트로 변환합니다.
tmdb_dataset['keywords'] = tmdb_dataset['keywords'].apply(ast.literal_eval)
# 'keywords' 컬럼에서 'name'에 해당하는 값만 추출하여 새로운 컬럼을 만듭니다.
tmdb_dataset['keyword_names'] = tmdb_dataset['keywords'].apply(lambda x: [item['name'] for item in x] if isinstance(x, list) else [])
# 'keywords' 컬럼과 불필요한 컬럼들을 삭제합니다.
df = tmdb_dataset.drop(['keywords'], axis=1)

# 각 키워드를 하나의 문자열로 합쳐서 컬럼에 저장
df['keyword_names'] = df['keyword_names'].apply(lambda keywords: ', '.join(keywords) if keywords else '')

# 필요한 컬럼만 선택
result_df = df[['original_title', 'genre_names', 'budget', 'popularity', 'release_date', 'revenue', 'runtime', 'vote_average', 'vote_count', 'keyword_names']]
result_df.to_csv('output_result.csv', index=False)
# 결과 출력
print(result_df.head())
