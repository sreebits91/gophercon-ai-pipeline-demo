from embedding_simulator import (
    EmbeddingSimulator
)


def main():

    simulator = EmbeddingSimulator(
        "config.json"
    )


    document = (
        "Artificial intelligence "
        "requires scalable pipelines"
    )


    result = simulator.generate_embedding(
        document
    )


    print(
        "Dimension:",
        result["dimension"]
    )

    print(
        "Processing time:",
        result["processing_time_ms"],
        "ms"
    )


    print(
        "First 5 values:"
    )

    print(
        result["embedding"][:5]
    )


if __name__ == "__main__":
    main()