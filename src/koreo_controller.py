import asyncio
import json
import logging
import os

import uvloop


class JsonFormatter(logging.Formatter):
    def format(self, record):
        record_dict = {
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "time": self.formatTime(record),
        }
        return json.dumps(record_dict)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", logging.INFO)
logger.setLevel(log_level)
logger.addHandler(handler)

lib_log_level = os.getenv("LIB_LOG_LEVEL", logging.WARNING)
logging.getLogger(name="httpcore.connection").setLevel(lib_log_level)
logging.getLogger(name="httpcore.http11").setLevel(lib_log_level)
logging.getLogger(name="httpx").setLevel(lib_log_level)
logging.getLogger(name="kr8s._api").setLevel(lib_log_level)
logging.getLogger(name="kr8s._auth").setLevel(lib_log_level)

import uvloop

from controller import controller_main
from controller.status import status_main

DIAGNOSTICS = os.environ.get("DIAGNOSTICS", "disabled")


async def main():
    if DIAGNOSTICS.lower().strip() != "enabled":
        return await controller_main()

    # Not 100% certain what the limit should be, perhaps higher.
    telemetry_sink: asyncio.Queue | None = asyncio.Queue(100)

    try:
        async with asyncio.TaskGroup() as main_tg:
            main_tg.create_task(controller_main(telemetry_sink=telemetry_sink))
            main_tg.create_task(status_main(telemetry_sink=telemetry_sink))
    except KeyboardInterrupt:
        logger.info("Initiating shutdown due to user-request.")

    except asyncio.CancelledError:
        logger.info("Initiating shutdown due to cancel.")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        logger.info("Initiating shutdown due to user stop.")
        exit(0)

    except asyncio.CancelledError:
        logger.info("Initiating shutdown due to cancel.")

    exit(1)
