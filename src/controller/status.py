import asyncio
import copy
import logging
import os
import time

import uvicorn

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from koreo import status

logger = logging.getLogger("controller.status")

PORT = int(os.environ.get("PORT", 5000))


async def koreo_cache_status(_):
    return JSONResponse(status.list_resources())


def workflow_events_status(telemetry: dict):
    async def _handler(_):
        if not telemetry:
            return JSONResponse({"error": "No telemetry data"})

        return JSONResponse(telemetry.get("event_watcher"))

    return _handler


def resource_events_status(telemetry: dict):
    async def _handler(_):
        if not telemetry:
            return JSONResponse({"error": "No telemetry data"})

        status_request_time = time.monotonic()

        scheduler_telemetry = copy.copy(telemetry.get("scheduler"))
        if not scheduler_telemetry:
            return JSONResponse({})

        schedule = scheduler_telemetry.get("schedule")
        if schedule:
            scheduler_telemetry["schedule"] = [
                [f"{scheduled_for - status_request_time:.4f}", *rest]
                for scheduled_for, *rest in schedule
            ]

        return JSONResponse(scheduler_telemetry)

    return _handler


async def aggregator(
    telemetry_sink: asyncio.Queue | None = asyncio.Queue(),
    telemetry_data: dict | None = None,
):
    if not telemetry_sink or telemetry_data is None:
        return

    while True:
        try:
            telemetry_update = await telemetry_sink.get()
            # We don't want to retry if these fail to process.
            telemetry_sink.task_done()

            telemetry_data["__last_update__"] = time.monotonic()

            update_source = telemetry_update.get("source")
            if not update_source:
                logging.debug(
                    f"Malformed controller telemetry data ({telemetry_update})"
                )
                continue

            telemetry_data[update_source] = telemetry_update.get("telemetry")
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise


async def status_main(telemetry_sink: asyncio.Queue | None = None):
    logger.info("Koreo Status Server Starting")

    telemetry_data = {"__system_start__": time.monotonic()}

    app = Starlette(
        debug=True,
        routes=[
            Route("/koreo/cache", koreo_cache_status),
            Route("/controller/events", workflow_events_status(telemetry_data)),
            Route("/controller/scheduler", resource_events_status(telemetry_data)),
        ],
    )

    config = uvicorn.Config(app, port=PORT, log_level="info")
    server = uvicorn.Server(config)

    try:
        async with asyncio.TaskGroup() as main_tg:
            main_tg.create_task(
                aggregator(
                    telemetry_sink=telemetry_sink, telemetry_data=telemetry_data
                ),
                name="telemetry-aggregator",
            )
            main_tg.create_task(server.serve(), name="status-server")
    except KeyboardInterrupt:
        logger.info("Initiating shutdown due to user stop.")
        exit(0)

    except asyncio.CancelledError:
        logger.info("Initiating shutdown due to cancel.")

    exit(1)
