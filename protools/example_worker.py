import asyncio
import logging
import random
from datetime import datetime

import uvloop

from protools.client.common import BaseProcessingWorker
from protools.client.common import WorkerServer
from protools.logs import DEFAULT_LOGGER_NAME
from protools.logs import setup_logger
from protools.options import WorkerOptionEnum


class WorkerCurrentTime(BaseProcessingWorker):
    """Fast result method.
    """

    name = "current_time"

    options = [
        WorkerOptionEnum.AUTH
    ]

    def processing(self, *args, **kwargs):
        return {
            "dt": datetime.now().isoformat()
        }


class WorkerAuthCheck(BaseProcessingWorker):
    """Auth all.
    """

    options = [
        WorkerOptionEnum.AUTH_BACKEND
    ]

    def processing(self, *args, **kwargs):
        return {
            "ok": True,
            "user": {"id": random.randint(100, 1000)}
        }


class WorkerCreateVector(BaseProcessingWorker):
    """First step.
    """

    name = "create_vector"

    next_step = "avg_value"

    def processing(self, size: int = 100, **kwargs):
        return {
            "vector": [random.random() for _ in range(size)]
        }


class WorkerAvgValue(BaseProcessingWorker):
    """Next async step.
    """

    name = "avg_value"

    def processing(self, vector: list, **kwargs):
        val = sum(vector) / len(vector)
        self.logger.info("Avg: %f", val)
        return {"value": val}


def run_forever():
    """Run as:
    python -c "from protools.example_worker import run_forever;run_forever()"
    """
    uvloop.install()
    setup_logger()
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    server = WorkerServer(
        methods=[
            WorkerAvgValue,
            WorkerCreateVector,
            WorkerCurrentTime,
            WorkerAuthCheck,
        ],
        logger=logger)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.run())

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(server.broker_connection.close())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
