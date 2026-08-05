PYTHON ?= python

.PHONY: help dataset dataset-small dataset-medium dataset-large clean


help:
	@echo "Available commands:"
	@echo "  make dataset        Generate default dataset"
	@echo "  make dataset-small  Generate 1K documents"
	@echo "  make dataset-medium Generate 100K documents"
	@echo "  make dataset-large  Generate 1M documents"


dataset:
	$(PYTHON) dataset/generate.py


dataset-small:
	$(PYTHON) dataset/generate.py \
		--count 1000 \
		--sentences 5 \
		--output dataset/sample_1k.json


dataset-medium:
	$(PYTHON) dataset/generate.py \
		--count 100000 \
		--sentences 10 \
		--output dataset/sample_100k.json


dataset-large:
	$(PYTHON) dataset/generate.py \
		--count 1000000 \
		--sentences 15 \
		--output dataset/sample_1m.json


clean:
	rm -f dataset/*.json