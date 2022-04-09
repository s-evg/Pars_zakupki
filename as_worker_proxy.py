import time

import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from fake_useragent import UserAgent
from aiohttp import ClientSession
from aiohttp_proxy import ProxyConnector, ProxyType
import asyncio


url = "https://free-proxy-list.net/"
headers = {'user-agent': UserAgent().chrome,
           'accept': '*/*'
           }


def proxy_list():
    """Парсим список доступных IP адресов."""

    response = requests.get(url=url, headers=headers)
    while True:
        if response.status_code == 200:
            soup = bs(response.content, 'html.parser')
            proxys = soup.find('textarea').get_text().split('\n')[3:-1]
            return proxys
        else:
            print('Не удалось получить список IP\nПереподключение через 3 сек')
            time.sleep(3)


bad_proxies = [] # список для плохих прокси


work_proxy = []
ban = []
bad = []


async def as_get_proxy(proxy):
    url = 'http://icanhazip.com/'

    conn = ProxyConnector.from_url(f"https://{proxy}")

    async with ClientSession(connector=conn) as session:

        try:
            async with session.get(url=url, headers=headers, timeout=3) as response:
                statuscode = response.status
                if statuscode == 200:
                    work_proxy.append(proxy)
                    print("Добавил: ", proxy)
                else:
                    bad.append(proxy)
                return (work_proxy)
        except Exception as ex:
            print(f"Bad proxy {proxy}")


async def semaphore(sem, proxy):
    async with sem:
        return await as_get_proxy(proxy)


async def gather_data(ban=[], bad=[]):
    """Подготавливаем задачи для запросов"""

    # print(f'Плохие прокси: {bad}')
    proxies = set(proxy_list())  # преобразуем список во множество
    # получаем список IP адресов получивших бан, и выкидываем их из спика прокси

    proxies = proxies - set(ban) - set(bad)

    tasks = []
    sem = asyncio.Semaphore(50)

    for proxy in proxies:
        task = asyncio.create_task(semaphore(sem, proxy))
        tasks.append(task)

    work_proxies = [await prox for prox in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
    print(work_proxy)
    return work_proxy


def my_ip():
    """Проверяем свой IP"""

    response = requests.get('http://icanhazip.com/', timeout=2)
    return response.text.strip()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gather_data())


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
