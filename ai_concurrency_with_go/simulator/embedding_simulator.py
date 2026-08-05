"""
Embedding Simulator

Provides a controlled AI workload
for benchmarking concurrency patterns.

This does NOT represent a real ML model.
It simulates the behaviour of an
embedding generation service.
"""


import json
import random
import time
import hashlib
from pathlib import Path


class EmbeddingSimulator:

    def __init__(self, config_path="config.json"):

        config_file = Path(config_path)

        with open(config_file, "r") as file:
            config = json.load(file)

        self.dimension = config[
            "embedding_dimension"
        ]

        self.latency_ms = config[
            "latency_ms"
        ]

        self.failure_rate = config[
            "failure_rate"
        ]

        random.seed(
            config["seed"]
        )


    def generate_embedding(self, text: str):
        """
        Generate simulated embedding.
        """

        start = time.time()


        # Simulate model processing time
        time.sleep(
            self.latency_ms / 1000
        )


        # Simulate failure
        if random.random() < self.failure_rate:
            raise Exception(
                "Embedding generation failed"
            )


        vector = self._create_vector(
            text
        )


        processing_time = (
            time.time() - start
        )


        return {
            "embedding": vector,
            "dimension": self.dimension,
            "processing_time_ms":
                round(
                    processing_time * 1000,
                    2
                )
        }


    def _create_vector(
        self,
        text: str
    ):

        """
        Deterministic pseudo embedding.

        Same text -> same vector.
        """

        hash_value = hashlib.sha256(
            text.encode()
        ).digest()


        random.seed(hash_value)


        return [
            round(
                random.random(),
                6
            )
            for _ in range(
                self.dimension
            )
        ]