from __future__ import annotations

import json
import random
import signal
import time
from dataclasses import dataclass
from typing import Any, Dict

from kafka import KafkaProducer

from transport_platform.core.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from transport_platform.core.logger import logger

SLEEP_SECONDS = 2
STATIONS = ["Paris", "Lyon", "Marseille", "Nice", "Lille"]


@dataclass(frozen=True)
class ProducerState:
    """Holds running state for graceful shutdown."""
    running: bool = True


def _handle_stop(_sig: int, _frame: Any, state: ProducerState) -> None:
    """Signal handler to stop the producer loop."""
    object.__setattr__(state, "running", False)  # type: ignore[misc]


def build_event() -> Dict[str, Any]:
    """Create a random event payload."""
    return {
        "station": random.choice(STATIONS),
        "delay_minutes": random.randint(0, 15),
        "timestamp": time.time(),
    }


def run() -> None:
    """Run the producer loop."""
    state = ProducerState()

    # Register signal handlers
    signal.signal(signal.SIGINT, lambda s, f: _handle_stop(s, f, state))
    signal.signal(signal.SIGTERM, lambda s, f: _handle_stop(s, f, state))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    logger.info("Kafka producer started (bootstrap=%s, topic=%s).", KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    try:
        while state.running:
            event = build_event()
            producer.send(KAFKA_TOPIC, event)
            logger.info("Sent event: %s", event)
            time.sleep(SLEEP_SECONDS)
    finally:
        logger.info("Stopping producer...")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        logger.info("Producer stopped.")


if __name__ == "__main__":
    run()