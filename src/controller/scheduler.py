from typing import NamedTuple, Protocol, TypeVar
import asyncio
import heapq
import logging
import random
import time

from koreo.result import PermFail, UnwrappedOutcome, is_error

logger = logging.getLogger("koreo.controller.scheduler")


class WorkProcessor[T](Protocol):
    async def __call__(
        self,
        payload: T,
        ok_frequency_seconds: int,
        sys_error_retries: int,
        user_retries: int,
    ) -> UnwrappedOutcome:
        pass


class Configuration[T](NamedTuple):
    work_processor: WorkProcessor[T]

    concurrency: int = 5

    frequency_seconds: int = 1200
    timeout_seconds: int = 30

    retry_max_retries: int = 10
    retry_delay_base: int = 10
    retry_delay_max: int = 300
    retry_delay_jitter: int = 30


class Request[T](NamedTuple):
    at: float  # Timestamp to, approximately, run at

    payload: T

    user_retries: int = 0
    sys_error_retries: int = 0


class Shutdown(NamedTuple):
    at: float = 0


RequestPayload = TypeVar("RequestPayload")
RequestQueue = asyncio.PriorityQueue[Request[RequestPayload] | Shutdown]


async def orchestrator[T](
    tg: asyncio.TaskGroup,
    requests: RequestQueue[T],
    configuration: Configuration[T],
):
    request_schedule: list[Request[T]] = []
    heapq.heapify(request_schedule)

    workers: set[asyncio.Task] = set()
    workers_spawned = 0

    work: asyncio.Queue[Request[T] | Shutdown] = asyncio.Queue()

    while True:
        # Ensure reconcilers are running
        if len(workers) < max(configuration.concurrency, 1):
            worker_task = tg.create_task(
                _worker_task(
                    work=work,
                    requests=requests,
                    configuration=configuration,
                ),
                name=f"worker-{workers_spawned}",
            )
            workers.add(worker_task)
            worker_task.add_done_callback(workers.remove)
            workers_spawned += 1
            continue

        if not request_schedule:
            # This delay is to ensure the system is self-maintaining
            next_scheduled_work = 120
        else:
            up_next = heapq.heappop(request_schedule)
            next_scheduled_work = up_next.at - time.monotonic()
            if next_scheduled_work < 1:
                await work.put(up_next)
                continue

            # Put next back into the queue so it gets picked up next time
            heapq.heappush(request_schedule, up_next)

        try:
            new_work_request = await asyncio.wait_for(
                requests.get(), timeout=next_scheduled_work
            )
            if isinstance(new_work_request, Shutdown):
                # Shutdown and clear the requests queue.
                requests.task_done()
                requests.shutdown(immediate=True)

                # Clear any pending work from the queue.
                while True:
                    try:
                        work.get_nowait()
                    except (asyncio.QueueEmpty, asyncio.QueueShutDown):
                        break

                # Add a shutdown for each worker.
                for _ in workers:
                    try:
                        work.put_nowait(Shutdown())
                    except (asyncio.QueueFull, asyncio.QueueShutDown):
                        pass

                # Shutdown the queue.
                work.shutdown()
                return

            heapq.heappush(request_schedule, new_work_request)
            requests.task_done()
        except asyncio.TimeoutError:
            # Indicates it is time to run the next scheduled reconciliation
            continue


async def _worker_task[T](
    work: asyncio.Queue[Request[T] | Shutdown],
    requests: asyncio.Queue[Request[T] | Shutdown],
    configuration: Configuration,
):
    while True:
        if shutdown := await _worker(
            work=work,
            requests=requests,
            configuration=configuration,
        ):
            return


async def _worker[T](
    work: asyncio.Queue[Request[T] | Shutdown],
    requests: asyncio.Queue[Request[T] | Shutdown],
    configuration: Configuration,
):
    request = await work.get()
    if isinstance(request, Shutdown):
        # This is normally done in the try's finally block.
        work.task_done()
        return True

    try:
        result = await asyncio.wait_for(
            configuration.work_processor(
                request.payload,
                ok_frequency_seconds=configuration.frequency_seconds,
                sys_error_retries=request.sys_error_retries,
                user_retries=request.user_retries,
            ),
            timeout=configuration.timeout_seconds,
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise

    except:
        logger.exception(
            f"Exception within {configuration.work_processor.__qualname__} "
            f"({request.sys_error_retries} retries)"
        )

        delay = min(
            (2**request.sys_error_retries) * configuration.retry_delay_base
            + random.randint(0, configuration.retry_delay_jitter),
            configuration.retry_delay_max,
        )

        next_attempt_at = time.monotonic() + delay
        sys_error_retries = request.sys_error_retries + 1

        if sys_error_retries < configuration.retry_max_retries:
            await requests.put(
                Request(
                    at=next_attempt_at,
                    payload=request.payload,
                    sys_error_retries=sys_error_retries,
                    user_retries=0,
                )
            )
        else:
            # TODO: Exhausted retries... what now?
            logger.error(
                f"Exhausted retry limit within {configuration.work_processor.__qualname__} "
                f"({request.sys_error_retries} retries)"
            )

        return False

    finally:
        work.task_done()

    if not is_error(result):
        # Ok, Skip, or DepSkip. Recheck at scheduled frequency.
        await requests.put(
            Request(
                at=time.monotonic() + configuration.frequency_seconds,
                payload=request.payload,
                sys_error_retries=0,
                user_retries=0,
            )
        )
        return False

    if isinstance(result, PermFail):
        logger.info(
            f"PermFail from {configuration.work_processor.__qualname__} "
            f"({request.user_retries} retries)"
        )
        return False

    # User-requested Retry case
    reconcile_at = time.monotonic() + result.delay

    await requests.put(
        Request(
            at=reconcile_at,
            payload=request.payload,
            user_retries=request.user_retries + 1,
            sys_error_retries=0,
        )
    )
