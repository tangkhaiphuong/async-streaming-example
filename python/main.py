import asyncio
from asyncio import CancelledError
from datetime import datetime
from queue import Queue
from typing import Set, Union, Callable, AsyncIterator, cast, TypeVar, Generic


async def main():
    concurrency = 4
    is_cancel = False

    async for item in crawl("https://golang.org/", concurrency, lambda: is_cancel):
        print(
            f'[{item[1]}] ({item[0].isoformat()}) {item[2]} => {item[3] if item[4] is None else item[4]}')


T = TypeVar("T")


class Channel(Generic[T]):
    __capacity: int
    __queue: Queue[T]

    def __init__(self, capacity: int):
        self.__capacity = capacity
        self.__queue = Queue()

    async def write(self, value: T, is_cancel: Callable[[], bool]) -> bool:
        while self.__queue.qsize() >= self.__capacity:
            if is_cancel():
                return False
            await asyncio.sleep(0)
        self.__queue.put(value)
        return True

    async def read(self, is_cancel: Callable[[], bool]) -> T:
        while self.__queue.empty():
            if is_cancel():
                raise CancelledError()
            await asyncio.sleep(0)
        value = self.__queue.get()
        return value


async def crawl(url: str, concurrency: int,
                is_cancel: Callable[[], bool]) -> AsyncIterator[
    tuple[datetime, str, str, Union[str, None], Union[Exception, None]]]:
    visited_url: Set[str] = set()

    url_queue: Queue[str] = Queue()
    url_channel: Channel[Union[str, None]] = Channel(concurrency)
    urls_channel: Channel[list[str]] = Channel(concurrency)
    result_channel: Channel[tuple[datetime, str, str, Union[str, None], Union[Exception, None]]] = Channel(concurrency)

    await url_channel.write(url, is_cancel)

    tasks: list[asyncio.Task] = []
    counter = 1

    async def worker(name: str):

        while True:
            processing_url = await url_channel.read(is_cancel)

            if processing_url is None:
                break

            timestamp = datetime.now()
            try:
                visited_url.add(processing_url)
                response = await fetch(processing_url)

                valid_urls = []
                for checking_url in response[1]:
                    if checking_url not in visited_url:
                        valid_urls.append(checking_url)

                await urls_channel.write(valid_urls, is_cancel)

                await result_channel.write((timestamp, name, processing_url, response[0], None), is_cancel)

            except Exception as exception:
                await urls_channel.write([], is_cancel)
                await result_channel.write((timestamp, name, processing_url, None, exception), is_cancel)

    for index in range(0, concurrency):
        tasks.append(asyncio.create_task(worker(f'Worker:{index + 1}')))

    while True:
        item = await result_channel.read(is_cancel)

        if item is None:
            break

        yield item

        counter -= 1

        urls = await urls_channel.read(is_cancel)

        if urls is None:
            break

        for url in urls:
            url_queue.put(url)

        if counter == 0 and url_queue.empty():
            break

        while counter < concurrency and not url_queue.empty():
            url = url_queue.get()
            await url_channel.write(url, is_cancel)
            counter += 1

    for index in range(0, concurrency):
        await url_channel.write(None, is_cancel)

    await asyncio.gather(*tasks)


async def fetch(url: str) -> tuple[str, list[str]]:
    result = fetcher.get(url)
    if result is None:
        await asyncio.sleep(500 / 1000)
        raise Exception('Not found')

    await asyncio.sleep(cast(int, result.get("elapsed")) / 1000)
    return cast(str, result.get("body")), cast(list[str], result.get("urls"))


fetcher: dict[str, dict[str, Union[str, int, list[str]]]] = {
    "https://golang.org/": {
        "body": "The Go Programming Language",
        "elapsed": 1000,
        "urls": [
            "https://golang.org/pkg/",
            "https://golang.org/cmd/",
            "https://golang.org/internal/",
        ],
    },
    "https://golang.org/internal/": {
        "body": "Packages internal",
        "elapsed": 1000,
        "urls": ["https://golang.org/"],
    },
    "https://golang.org/pkg/": {
        "body": "Packages",
        "elapsed": 1000,
        "urls": [
            "https://golang.org/",
            "https://golang.org/cmd/",
            "https://golang.org/pkg/fmt/",
            "https://golang.org/pkg/os/",
            "https://golang.org/pkg/container/",
        ],
    },
    "https://golang.org/pkg/fmt/": {
        "body": "Package fmt",
        "elapsed": 1000,
        "urls": ["https://golang.org/", "https://golang.org/pkg/"],
    },
    "https://golang.org/pkg/container/": {
        "body": "Package container",
        "elapsed": 1000,
        "urls": [
            "https://golang.org/pkg/",
            "https://golang.org/pkg/container/list/",
            "https://golang.org/pkg/container/heap/",
        ],
    },
    "https://golang.org/pkg/container/list/": {
        "body": "Package list",
        "elapsed": 1000,
        "urls": ["https://golang.org/pkg/container/"],
    },
    "https://golang.org/pkg/container/heap/": {
        "body": "Package heap",
        "elapsed": 1000,
        "urls": ["https://golang.org/pkg/container/"],
    },
    "https://golang.org/pkg/os/": {
        "body": "Package os",
        "elapsed": 1000,
        "urls": ["https://golang.org/", "https://golang.org/pkg/"],
    },
}

if __name__ == "__main__":
    asyncio.run(main())
