# Settings shared by all three services.

RABBITMQ_HOST = "localhost"

# Queues
DOCUMENT_STORED_QUEUE = "document.stored"  # Document service -> Embedding service
EMBEDDING_RESULTS_QUEUE = "embedding.results"  # Embedding service -> Document service

# Service URLs (used when one service calls another over HTTP)
QUOTA_URL = "http://localhost:8081"
