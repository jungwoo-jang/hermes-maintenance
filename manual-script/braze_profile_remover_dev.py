import asyncio
import copy
import csv
import logging
import time
from typing import Any, Coroutine, Dict, List, Set

import aiohttp
from aiohttp import ClientResponse

# Braze
BRAZE_API_URL: str = "https://rest.iad-05.braze.com/users/delete"
BRAZE_API_KEY: str = "215eebf9-ce77-42f6-863d-aa651bd9c546"
MAX_EXTERNAL_IDS_PER_REQUEST: int = 50
API_RATE_LIMIT_PER_MINUTE: int = 20000
API_CALL_SLEEP_SECONDS: int = 60 + 10

# CSV
CSV_FILE_NAME: str = "null_external_ids.csv"

# Logger
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S%z"
)
logger = logging.getLogger(__name__)

deleted_external_ids: Set[str] = set()
failed_to_delete_external_ids: Set[str] = set()


async def call_api(session: aiohttp.ClientSession, external_ids: Set[str]) -> None:
    headers: Dict[str, Any] = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {BRAZE_API_KEY}",
    }
    data: Dict[str, Any] = {"external_ids": list(external_ids)}

    response: ClientResponse = await session.post(url=BRAZE_API_URL, headers=headers, json=data)
    if response.ok:
        deleted_external_ids.update(external_ids)
    else:
        logger.error(response.status)
        failed_to_delete_external_ids.update(external_ids)


def get_external_ids() -> Set[str]:
    external_ids: Set[str] = set()
    with open(CSV_FILE_NAME, newline="") as csvfile:
        for row in csv.reader(csvfile):
            external_ids.add(row[0])

    return external_ids


async def get_tasks(session: aiohttp.ClientSession, external_ids: Set[str]) -> List[Coroutine]:
    tasks: List[Coroutine] = []
    chunk_of_external_ids: Set[str] = set()
    for external_id in external_ids:
        chunk_of_external_ids.add(external_id)
        if len(chunk_of_external_ids) == MAX_EXTERNAL_IDS_PER_REQUEST:
            tasks.append(call_api(session, copy.deepcopy(chunk_of_external_ids)))
            chunk_of_external_ids.clear()

    if len(chunk_of_external_ids) > 0:
        tasks.append(call_api(session, copy.deepcopy(chunk_of_external_ids)))

    return tasks


async def main() -> None:
    start: float = time.time()
    external_ids: Set[str] = get_external_ids()
    number_of_deleted_external_ids: int = 0
    logger.info("Number of braze profiles that need to be deleted: %d", len(external_ids))
    async with aiohttp.ClientSession() as session:
        tasks: List[Coroutine] = await get_tasks(session, external_ids)
        logger.info("Number of requests to '/users/delete': %d", len(tasks))
        chunk_of_tasks: List[Coroutine] = []
        logger.info("Please wait until done...")
        for task in tasks:
            chunk_of_tasks.append(task)
            if len(chunk_of_tasks) == API_RATE_LIMIT_PER_MINUTE:
                await asyncio.gather(*chunk_of_tasks)
                number_of_deleted_external_ids += MAX_EXTERNAL_IDS_PER_REQUEST * API_RATE_LIMIT_PER_MINUTE
                logger.info(
                    "[%d braze profiles are deleted... sleep %d seconds for api rate limit...]",
                    number_of_deleted_external_ids,
                    API_CALL_SLEEP_SECONDS
                )
                await asyncio.sleep(API_CALL_SLEEP_SECONDS)
                chunk_of_tasks.clear()

        if len(chunk_of_tasks) > 0:
            await asyncio.gather(*chunk_of_tasks)
    logger.info("Tasks done!")
    logger.info("Number of deleted braze profiles: %d", len(deleted_external_ids))
    logger.error("Number of failed to delete braze profiles: %d", len(failed_to_delete_external_ids))
    logger.info("Elapsed time: %.2f sec", time.time() - start)


if __name__ == "__main__":
    asyncio.run(main())
