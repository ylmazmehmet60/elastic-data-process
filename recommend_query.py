from elasticsearch import Elasticsearch
import logging

class MovieSearch:
    def __init__(self, es):
        self.es = es

    def search_movie_title(self, movie_id):
        query = {"query": {"match": {"movieId.keyword": {"query": movie_id}}}, "_source": ["title"]}
        try:
            result = self.es.search(index="movies", body=query)
            title = result['hits']['hits'][0]['_source']['title']
            return title
        except Exception as e:
            logging.error(f"Error in search_movie_title: {e}")
            return None

    def search_movie_id(self, title):
        query = {"query": {"match": {"title.keyword": {"query": title}}}, "size": 1, "_source": ["movieId"]}
        try:
            result = self.es.search(index="movies", body=query)
            return int(result['hits']['hits'][0]['_source']['movieId']) if result['hits']['hits'] else None
        except Exception as e:
            logging.error(f"Error in search_movie_id: {e}")
            return None

    def search_recommendations(self, movie_id):
        query = {"query": {"match": {"movieId": {"query": movie_id}}}, "size": 1, "_source": ["recommendations.userId"]}
        try:
            result = self.es.search(index="item_recommendation", body=query)
            return result['hits']['hits'][0]['_source']['recommendations'] if result['hits']['hits'] else None
        except Exception as e:
            logging.error(f"Error in search_recommendations: {e}")
            return None

    def search_highly_rated_movies(self, user_id):
        titles = []
        query = {
            "query": {
                "bool": {
                    "must": [{"term": {"userId.keyword": user_id}}, {"range": {"rating.keyword": {"gte": "5"}}}]
                }
            },
            "sort": [{"rating.keyword": {"order": "desc"}}]
        }
        try:
            result = self.es.search(index="ratings", body=query)
            for r in result.get('hits', {}).get('hits', []):
                title = self.search_movie_title(r['_source']['movieId'])
                if title:
                    titles.append(title)
        except Exception as e:
            logging.error(f"Error in search_highly_rated_movies: {e}")
        return titles

def main():
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    logging.basicConfig(level=logging.INFO)
    
    movie_search = MovieSearch(es)

    title = "Prometheus (2012)"
    movie_id = movie_search.search_movie_id(title)

    if movie_id is not None:
        recommended_titles = []
        for item in movie_search.search_recommendations(movie_id) or []:
            user_id = item.get('userId')
            if user_id is not None:
                recommended_titles.extend(movie_search.search_highly_rated_movies(user_id))

        for recommended_title in recommended_titles:
            logging.info(f"Recommended Movie Title: {recommended_title}")

if __name__ == "__main__":
    main()
