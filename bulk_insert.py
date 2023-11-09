from elasticsearch import Elasticsearch, helpers
import csv

class ElasticsearchHandler:
    def __init__(self, hosts=['http://localhost:9200']):
        self.es = Elasticsearch(hosts=hosts)

    def index_exists(self, index_name):
        return self.es.indices.exists(index=index_name)

    def create_index(self, index_name, num_shards=10, num_replicas=0):
        if not self.index_exists(index_name):
            body = {
                "settings": {
                    "number_of_shards": num_shards,
                    "number_of_replicas": num_replicas
                }
            }
            self.es.indices.create(index=index_name, body=body)
            print(f"Index '{index_name}' created with {num_shards} shards and {num_replicas} replicas.")

    def read_csv(self, file_path, index_name):
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            reader = csv.DictReader(file)
            buffer = []
            for row in reader:
                document = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_source": row
                }
                buffer.append(document)

                if len(buffer) >= 5000:
                    yield buffer
                    buffer = []
            if buffer:
                yield buffer

    def bulk_insert(self, files_and_indexes, num_shards=10, num_replicas=0):
        for file_path, index_name in files_and_indexes:
            if not self.index_exists(index_name):
                self.create_index(index_name, num_shards, num_replicas)

            for batch in self.read_csv(file_path, index_name):
                success, failed = helpers.bulk(
                    self.es,
                    batch,
                    raise_on_error=False,
                    request_timeout=30,
                    headers={'Content-Type': 'application/json'}
                )
                print(f"Index: {index_name}, Success: {success}, Failed: {failed}")

# Example usage:
files_and_indexes = [
    ('../ml-latest/movies.csv', 'movies'),
    ('../ml-latest/links.csv', 'links'),
    ('../ml-latest/tags.csv', 'tags')
]

es_handler = ElasticsearchHandler()
es_handler.bulk_insert(files_and_indexes, num_shards=10, num_replicas=0)