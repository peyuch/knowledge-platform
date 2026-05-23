"""RAPTOR Consumer process entry point.

Usage:
    python -m workers.tasks.raptor
"""

from services.raptor.consumer import RaptorConsumer


def main():
    consumer = RaptorConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
