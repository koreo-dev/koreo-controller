from typing import Awaitable
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
        telemetry_update = await telemetry_sink.get()
        # We don't want to retry if these fail to process.
        telemetry_sink.task_done()

        telemetry_data["__last_update__"] = time.monotonic()

        update_source = telemetry_update.get("source")
        if not update_source:
            logging.debug(f"Malformed controller telemetry data ({telemetry_update})")
            continue

        telemetry_data[update_source] = telemetry_update.get("telemetry")


async def _done_watcher(guard: asyncio.Event, task: Awaitable):
    try:
        return await task

    except KeyboardInterrupt:
        pass

    finally:
        guard.set()


class StatusServiceFailure(Exception):
    """Status service process exited unexpectedly."""

    pass


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

    config = uvicorn.Config(app, port=PORT, log_level="info", reload=False, workers=1)
    server = uvicorn.Server(config)

    shutdown_trigger = asyncio.Event()

    try:
        async with asyncio.TaskGroup() as main_tg:
            main_tg.create_task(
                _done_watcher(
                    guard=shutdown_trigger,
                    task=aggregator(
                        telemetry_sink=telemetry_sink, telemetry_data=telemetry_data
                    ),
                ),
                name="telemetry-aggregator",
            )
            main_tg.create_task(
                _done_watcher(guard=shutdown_trigger, task=server.serve()),
                name="status-server",
            )

            await shutdown_trigger.wait()
            logger.info("Status service task exited unexpectedly.")

            _task_cancelled = False
            for task in main_tg._tasks:
                if not task.done():
                    continue

                if task.cancelled():
                    _task_cancelled = True
                    continue

                if task.exception() is not None:
                    return

            if _task_cancelled:
                raise asyncio.CancelledError(
                    "Status service task cancelled unexpectedly."
                )

            raise StatusServiceFailure("Status service task exited unexpectedly.")

    except KeyboardInterrupt:
        logger.info("Status service shutdown due to user-request.")
        return

    except SystemExit:
        logger.info("Status service shutdown due to system exit.")
        return

    except (BaseExceptionGroup, ExceptionGroup) as errs:
        logger.error("Unhandled exception in status process main.")
        for idx, err in enumerate(errs.exceptions):
            logger.error(f"Error[{idx}]: {type(err)}({err})")
        raise
