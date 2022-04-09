#!/venv/bin/python
"""developer s-evg | https://github.com/s-evg"""
from pprint import pprint

from bs4 import BeautifulSoup as bs
import requests, time, random, logging
import openpyxl
from tqdm import tqdm
from appJar import gui
from aiohttp import ClientSession
import asyncio
import pandas as pd
from pathlib import Path
from fake_useragent import UserAgent
from collections import Counter
import lxml


current_date = time.strftime("%d-%m-%Y—%H-%M")


logging.basicConfig(
    level=logging.DEBUG,
    filename='INFO.log',
    filemode='w',
    format = "%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S'
)
logging.debug("ВНИМАНИЕ")
logging.info("ОК")
logging.error("ОШИБКА")


URL = "https://zakupki.gov.ru/epz/contract/search/results.html"

HEADERS = {'user-agent': UserAgent().chrome,
           'accept': '*/*'
           }

PARAMS = {
    "morphology": "on",
    "search-filter": "Дате+размещения",
    "fz44=on&contractStageList_0": "on",
    "contractStageList_1": "on",
    "contractStageList_2": "on",
    "contractStageList_3": "on",
    "contractStageList": "0%2C1%2C2%2C3",
    "contractCurrencyID": "-1",
    "budgetLevelsIdNameHidden": "%7B%7D",
    "publishDateFrom": "19.07.2021",
    "sortBy": "UPDATE_DATE",
    "pageNumber": "1",
    "sortDirection": "false",
    "recordsPerPage": "_50",
    "showLotsInfoHidden": "false"
}


#############################################################################################
####          Работа с исходным файлом: проверка и получение номеров аукционов           ####
#############################################################################################

file_extensions = [".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods", ".odt", ".csv"]


def check_file(src_file):
    """Проверяем выбран ли файл, и его расширение"""
    errors = False
    errors_msgs = []
    extension = None

    # проверяем что выбран файл
    if src_file == "":
        errors = False
        # errors_msgs.append("Не выбран файл.")
    else:
        # проверяем что расширение поддерживается
        extension = Path(src_file).suffix.lower()
        # print(extension)
        if extension not in file_extensions:
            errors = True
            errors_msgs.append("Выбран не поддерживаемый файл.")

    return(errors, errors_msgs, extension)


def auction_numbers(src_file, extension):
    """Открываем файл и получаем множество номеров аукционов"""

    if src_file == "":
        return []

    elif extension == ".csv":
        try:
            data_frame = pd.read_csv(src_file, sep=";", on_bad_lines="warn", encoding="utf-8", engine="python")

        except UnicodeDecodeError:
            data_frame = pd.read_csv(src_file, sep=";", on_bad_lines="warn", encoding="cp1252", engine="python")

    else:
        data_frame = pd.read_excel(src_file)

    column_names = data_frame.columns
    number = data_frame[column_names[0]].tolist()
    # print(type(number[0]) == int)
    if "№" in str(number[0]):
        number = [str(_.split()[-1]) for _ in number]
    else:
        number = [str(_) for _ in number]
    # print(type(number[0]))

    return number


#############################################################################################
####                 Подготовка задач и сбор информации о поставщиках                    ####
#############################################################################################


info_table = []


async def as_info(sem, session, link, auction, retry=2):
    """Получаем информацию поставщиков по множеству номеров аукционов"""

    try:
        async with session.get(url=link, headers=HEADERS, timeout=60) as response:
            # print(response.status)
            if response.status == 200:
                response_text = await response.text()
                time.sleep(0.03)
            elif response.status == 404:
                info_table.append({
                    "number": auction,
                    "name": "Страница отсутствует",
                    "inn": "Страница отсутствует",
                    "e-mail": "Страница отсутствует",
                    "phone": "Страница отсутствует"
                })
                return False
            else:
                print(f"Сайт не доступен. Ошибка «{response.status}» Проверьте подключение к интернету.")
                print(f"Переподключение через 30 секунд.\nСтраница {link} | retry={retry}")
                time.sleep(30)
                return await as_info(sem, session, link, auction, retry=(retry - 1))

    except Exception as ex:
        if retry:
            print(f"ОЙ!! Ошибка соединения. Переподключение через 30 секунд.\nСтраница {link} | retry={retry}")
            print(ex)
            time.sleep(30)
            return await as_info(sem, session, link, auction, retry=(retry - 1))
        else:
            info_table.append({
                "number": auction,
                "name": "ОШИБКА",
                "inn": "ОШИБКА",
                "e-mail": "ОШИБКА",
                "phone": "ОШИБКА"
            })
            return False

    else:
        return response_text


async def semaphore_info(sem, session, link, auction):
    async with sem:
        return await as_info(sem, session, link, auction)


async def gather_data(src_file, extension):
    """Подготавливаем задачи для запроса информации поставщиков"""

    url = "https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber="

    async with ClientSession() as session:
        auctions = await as_reestr_numbers(session)

        tasks = []
        sem = asyncio.Semaphore(5)

        auction_source = auction_numbers(src_file, extension)
        print(f"Собрано с сайта: {len(auctions)}\nВ базе: {len(auction_source)}")
        # counter = Counter(auction_site)
        # print(counter)
        auction_site_set = set(auctions)
        auction_source_set = set(auction_source)
        print(f"Уникально собрано с сайта: {len(auction_site_set)}")
        auctions = auction_site_set - auction_source_set
        # pprint(auctions)
        print(f"Новых аукционов: {len(auctions)}")

        for auction in auctions:
            link = f"{url}{auction}"
            task = asyncio.create_task(semaphore_info(sem, session, link, auction))
            tasks.append(task)

        print("\nСобираю информацию о поставщиках:")
        info_providers = [await info for info in tqdm(asyncio.as_completed(tasks), total=len(tasks))]

        print("\nРаспарсиваю информацию:")
        for info in tqdm(info_providers):

            if info:

                soup = bs(info, "lxml")
                num = soup.find("div", {"class": "navBreadcrumb__item navBreadcrumb__item_active"}).get_text().split()[1]
                providers = soup.findAll("div", {"class": "participantsInnerHtml"})

                if len(providers) == 0:
                    continue
                else:
                    for provider in providers:
                        if "Телефон, электронная почта" in provider.get_text():
                            td = provider.findAll("td")
                            name = td[0].get_text().split("\n")[1].strip()
                            email = td[-2].get_text().split("\n")[4].strip()
                            phone = td[-2].get_text().split("\n")[3].strip()
                            inn = td[0].get_text().split("\n\n")
                            for _ in inn:
                                if "инн" in _.lower():
                                    inn = _.split("\n")[-1]

                            info_table.append({
                                "number": num,
                                "name": name,
                                "inn": inn,
                                "e-mail": email,
                                "phone": phone
                            })

        # print(info_table)


#############################################################################################
####                  Подготовка и сбор номеров аукционов с сайта                        ####
#############################################################################################


async def get_page_data(sem, session, pageNumber, retry=2):
    """Собираем страницы аукционов"""
    PARAMS.update(pageNumber=pageNumber)

    try:
        async with session.get(url=URL, headers=HEADERS, params=PARAMS, timeout=60) as response:
            # print(response.status)
            if response.status == 200:
                response_text = await response.text()
                time.sleep(0.03)
            else:
                print(f"Сайт не доступен. Ошибка «{response.status}» Проверьте подключение к интернету.")
                print(f"Переподключение через 20 секунд.\nСтраница {pageNumber} | retry={retry}")
                time.sleep(20)
                return await get_page_data(sem, session, pageNumber, retry=(retry - 1))

    except Exception as ex:
        if retry:
            print(f"ОЙ!! Ошибка соединения. Переподключение через 20 секунд.\nСтраница {pageNumber} | retry={retry}")
            print(ex)
            time.sleep(20)
            return await get_page_data(sem, session, pageNumber, retry=(retry - 1))
        else:
            raise

    else:
        return response_text


async def semaphore_age_number(sem, session, pageNumber):
    async with sem:
        return await get_page_data(sem, session, pageNumber)


async def as_reestr_numbers(session):
    """Получаем номера аукционов со страницы с сайта"""

    start = time.time()
    tasks = []
    number_list = []

    sem = asyncio.Semaphore(3)

    for pageNumber in range(1, 101):
        task = asyncio.create_task(semaphore_age_number(sem, session, pageNumber))
        tasks.append(task)

    print("Готовлю список аукционов:")
    page_number = [await pn for pn in tqdm(asyncio.as_completed(tasks), total=len(tasks))]

    print("\nПолучаю номера аукционов:")
    for pn in tqdm(page_number):
        if pn is not None:
            soup = bs(pn, "lxml")
            number = soup.findAll("div", {"class": "registry-entry__header-mid__number"})
            for _ in number:
                link = _.find("a").get("href")
                reestr_number = link.split("=")[-1]
                number_list.append(reestr_number)
        else:
            number_list = []
        time.sleep(0.1)
    # print(number_list)

    print(f"Собрано {len(number_list)}  ссылок за {round((time.time() - start), 2)} секунд")
    return number_list


#############################################################################################
####                            Запись результатов в файл                                ####
#############################################################################################


def write(save_dir):
    """Записываем полученные данные в xlsx"""

    book = openpyxl.Workbook()
    sheet = book.active

    sheet['A1'] = 'Номер аукциона'
    sheet['B1'] = 'Наименование компании'
    sheet['C1'] = 'ИНН'
    sheet['D1'] = 'E-mail'
    sheet['E1'] = 'Телефон'

    row = 2

    for _ in info_table:
        sheet[row][0].value = _["number"]
        sheet[row][1].value = _["name"]
        sheet[row][2].value = _["inn"]
        sheet[row][3].value = _["e-mail"]
        sheet[row][4].value = _["phone"]
        row += 1

    while True:

        try:
            if save_dir == "":
                if app.getCheckBox("Добавить к имени файла текущую дату и время."):
                    book.save(f'new-zakupki-{current_date}.xlsx')
                else:
                    book.save(f'new-zakupki.xlsx')
            else:
                if app.getCheckBox("Добавить к имени файла текущую дату и время."):
                    book.save(f'{save_dir}/zakupki-{current_date}.xlsx')
                else:
                    book.save(f'{save_dir}/zakupki.xlsx')
            book.close()
            break

        except PermissionError:
            print('Кажется у Вас открыт файл в Exel,\nПожалуйста, закройте его, и тогда я смогу сохранить.')
            input('Закройте Exel, затем вернитесь сюда и нажмите ENTER')


#############################################################################################
####                            Обработка нажатий кнопок                                 ####
#############################################################################################


def press(button):
    """ Выполняет нажатие кнопки

    Аргументы:
        button: название кнопки. Используем названия Выполнить или Выход
    """
    start = time.time()

    loop = asyncio.get_event_loop()

    if button == "Старт":
        src_file = app.getEntry("Input_File")
        errors, error_msg, extension = check_file(src_file)
        if errors:
            app.errorBox("Ошибка", "\n".join(error_msg), parent=None)
        else:
            loop = asyncio.get_event_loop()
            auction_site = loop.run_until_complete(gather_data(src_file, extension))
            # auction_source = auction_numbers(src_file, extension)
            # print(f"Собрано: {len(auction_site)}\nВ базе: {len(auction_source)}")
            # # counter = Counter(auction_site)
            # # print(counter)
            # auction_site_set = set(auction_site)
            # auction_source_set = set(auction_source)
            # print(f"Уникально собрано с сайта: {len(auction_site_set)}")
            # auctions = auction_site_set - auction_source_set
            # pprint(auctions)
            # # print(f"Уникальных: {len(auctions)}")

            # info(auctions)

            save_dir = app.getEntry("Output_Directory")
            write(save_dir)

            print(f"Выполнено за {round((time.time() - start), 2)} секунд")
            print("Программу можно закрыть.")

    else:
        app.stop()


# Создать окно пользовательского интерфейса
app = gui(f"Закупки | {current_date}", useTtk=True)
# app.icon = ("icon.ico")
app.setTtkTheme("alt")
app.setSize(500, 200)

# Добавить интерактивные компоненты
app.addLabel("Выберите исходную базу. | Оставьте пустым для новой выгрузки.")
app.addFileEntry("Input_File")

app.addLabel("Выберите папку для сохранения")
app.addDirectoryEntry("Output_Directory")

# app.addLabel("Page Ranges: 1,3,4-10")
# app.addEntry("Page_Ranges")
cd = app.addCheckBox("Добавить к имени файла текущую дату и время.")

# Связать кнопки с функцией под названием press
app.addButtons(["Старт"], press)


# def main():
#     loop = asyncio.get_event_loop()
#     app.go()


if __name__ == "__main__":
    # start = time.time()
    app.go()
    # main()
    # loop = asyncio.get_event_loop()

    # loop.run_until_complete(gather_data())
    # write(save_dir="")
    # print(f"Выполнено за за {round((time.time() - start), 2)} секунд")
