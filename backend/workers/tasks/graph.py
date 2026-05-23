"""GraphRAG Consumer process entry point.

Usage:
    python -m workers.tasks.graph
"""

from services.graphrag.consumer import GraphRagConsumer


def main():
    consumer = GraphRagConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
