from aiohttp import ClientSession
from bs4 import BeautifulSoup
import time
import asyncio
import lxml
from tqdm import tqdm

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
}


def read_file():
    with open("books_urls_cop.txt") as file:
        books_urls = file.read().splitlines()
        return books_urls


async def get_page_data(sem, session, url, retry=5):

    try:
        async with session.get(url=url, headers=headers) as response:
            response_text = await response.text()
    except Exception as ex:
        if retry:
            print(f"[INFO] retry={retry} => {url}")
            time.sleep(20)
            return await get_page_data(sem, session, url, retry=(retry - 1))
        else:
            raise
    else:
        # print(f"[INFO] Обработал страницу {url}")
        return response_text


async def semaphore(sem, session, url):
    async with sem:
        return await get_page_data(sem, session, url)


async def gather_data():
    """Подготавливаем задачи для запросов"""

    books_urls = read_file()
    tasks = []

    sem = asyncio.Semaphore(25)

    async with ClientSession() as session:
        for url in books_urls:
            task = asyncio.create_task(semaphore(sem, session, url))
            tasks.append(task)

        books = [await book for book in tqdm(asyncio.as_completed(tasks), total=len(tasks))]

        for book in books:
            try:
                soup = BeautifulSoup(book, "lxml")
                print(f"{soup.title.text}\n{'-' * 77}")
            except Exception as ex:
                continue


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gather_data())


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Затрачено {round((time.time() - start), 2)} секунд")
