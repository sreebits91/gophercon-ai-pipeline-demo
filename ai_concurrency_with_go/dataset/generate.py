"""
Synthetic Document Dataset Generator

Purpose:
Generate a controlled dataset for benchmarking
AI embedding pipelines.

The same dataset will be consumed by:
- Python pipelines
- Go pipelines

to ensure fair comparison.
"""

import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path


TOPICS = [
    "artificial intelligence",
    "distributed systems",
    "cloud computing",
    "machine learning",
    "vector databases",
    "software engineering",
    "concurrency programming",
    "data pipelines",
    "large language models",
    "retrieval augmented generation",
]


SENTENCES = [
    "Modern applications require scalable architectures to handle increasing workloads.",
    "Distributed systems rely on efficient communication and resource management.",
    "Artificial intelligence applications require reliable data processing pipelines.",
    "Concurrency enables systems to process multiple tasks efficiently.",
    "Production systems require monitoring, fault tolerance, and graceful recovery.",
    "Vector embeddings enable semantic search and knowledge retrieval.",
    "Engineering decisions influence scalability and system performance.",
    "High throughput systems require careful resource management.",
    "Reliable pipelines require observability and failure handling mechanisms.",
]


def generate_document(document_id: int, sentence_count: int) -> dict:
    """
    Generate a single synthetic document.
    """

    topic = random.choice(TOPICS)

    content = " ".join(
        random.choice(SENTENCES)
        for _ in range(sentence_count)
    )

    return {
        "id": document_id,
        "title": f"{topic.title()} Document {document_id}",
        "content": content,
        "category": topic,
        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def generate_dataset(
    count: int,
    sentence_count: int,
    output_file: str,
):
    """
    Generate dataset and save as JSON.
    """

    documents = []

    for document_id in range(1, count + 1):
        documents.append(
            generate_document(
                document_id,
                sentence_count,
            )
        )

    output_path = Path(output_file)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            documents,
            file,
            indent=2,
        )

    print(
        f"""
Dataset generated successfully

Documents : {count}
Sentences : {sentence_count}
Output    : {output_path}
"""
    )


def main():

    parser = argparse.ArgumentParser(
        description="Generate synthetic AI benchmark dataset"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10000,
        help="Number of documents",
    )

    parser.add_argument(
        "--sentences",
        type=int,
        default=10,
        help="Sentences per document",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="dataset/sample_documents.json",
        help="Output JSON file",
    )

    args = parser.parse_args()

    generate_dataset(
        count=args.count,
        sentence_count=args.sentences,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()