lint:
	pylint src

run-producer:
	python -m transport_platform.ingestion.kafka_producer

run-stream:
	python -m transport_platform.streaming.spark_structured_stream

build-silver:
	python -m transport_platform.batch.build_silver

build-gold:
	python -m transport_platform.batch.build_gold_features
