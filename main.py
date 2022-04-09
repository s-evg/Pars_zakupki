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


current_date = time.strftime("%d-%m-%Y")
file_extensions = [".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods", ".odt", ".csv"]


logging.basicConfig(
    level=logging.DEBUG,
    filename='zakupki.log',
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


def pars(pageNumber):

    PARAMS.update(pageNumber=pageNumber)
    connection_attempt = 0

    while connection_attempt < 5:

        try:
            response = requests.get(url=URL, headers=HEADERS, params=PARAMS, timeout=3)
            print(response)
            break

        except requests.ConnectionError as e:
            print("ОЙ!! Ошибка соединения. Убедитесь что подключены к интернету.\n")
            print(str(e))
            connection_attempt += 1
            print(f"Попытка подключения {connection_attempt} из 5")
            time.sleep(5)
        except requests.Timeout as e:
            print("ОЙ!! Ошибка тайм-аута")
            print(str(e))
            connection_attempt += 1
            print(f"Попытка подключения {connection_attempt} из 5")
            time.sleep(5)
        except requests.RequestException as e:
            print("ОЙ!! Общая ошибка")
            print(str(e))
            connection_attempt += 1
            print(f"Попытка подключения {connection_attempt} из 5")
            time.sleep(5)
        except KeyboardInterrupt:
            print("Кто-то закрыл программу")

    else:
        response = None

    return response


info_table = []


def info(auctions):
    """Получаем информацию поставщиков по множеству номеров аукционов"""
    url = f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber="

    for num in tqdm(auctions):
        print(num)
        connection_attempt = 0

        while connection_attempt < 5:

            try:
                responce = requests.get(url=f"{url}{num}", headers=HEADERS, timeout=3)
                soup = bs(responce.content, "html.parser")
                providers = soup.findAll("div", {"class": "participantsInnerHtml"})
                break

            except requests.ConnectionError as e:
                print("ОЙ!! Ошибка соединения. Убедитесь что подключены к интернету.\n")
                print(str(e))
                connection_attempt += 1
                print(f"Попытка подключения {connection_attempt} из 5")
                time.sleep(5)
            except requests.Timeout as e:
                print("ОЙ!! Ошибка тайм-аута")
                print(str(e))
                connection_attempt += 1
                print(f"Попытка подключения {connection_attempt} из 5")
                time.sleep(5)
            except requests.RequestException as e:
                print("ОЙ!! Общая ошибка")
                print(str(e))
                connection_attempt += 1
                print(f"Попытка подключения {connection_attempt} из 5")
                time.sleep(5)
            except KeyboardInterrupt:
                print("Кто-то закрыл программу")
                connection_attempt += 1
                print(f"Попытка подключения {connection_attempt} из 5")
                time.sleep(5)

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

        if connection_attempt == 5:
            info_table.append({
                "number": num,
                "name": "ОШИБКА",
                "inn": "ОШИБКА",
                "e-mail": "ОШИБКА",
                "phone": "ОШИБКА"
            })

        time.sleep(0.333)
    return info_table


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
                if app.getCheckBox("Добавить к имени файла текущую дату"):
                    book.save(f'new-zakupki-{current_date}.xlsx')
                else:
                    book.save(f'new-zakupki.xlsx')
            else:
                if app.getCheckBox("Добавить к имени файла текущую дату"):
                    book.save(f'{save_dir}/zakupki-{current_date}.xlsx')
                else:
                    book.save(f'{save_dir}/zakupki.xlsx')
            book.close()
            break

        except PermissionError:
            print('Кажется у Вас открыт файл в Exel,\nПожалуйста, закройте его, и тогда я смогу сохранить.')
            input('Закройте Exel, затем вернитесь сюда и нажмите ENTER')


def check_file(src_file):
    """Проверяем выбран ли файл, и его расширение"""
    errors = False
    errors_msgs = []
    extension = None

    # проверяем что выбран файл
    if src_file == "":
        errors = True
        errors_msgs.append("Не выбран файл.")

    # проверяем что расширение поддерживается
    extension = Path(src_file).suffix.lower()
    print(extension)
    if extension not in file_extensions:
        errors = True
        errors_msgs.append("Выбран не поддерживаемый файл.")

    return(errors, errors_msgs, extension)


def auction_numbers(src_file, extension):
    """Открываем файл и получаем множество номеров аукционов"""
    if extension == ".csv":
        try:
            data_frame = pd.read_csv(src_file, sep=";", on_bad_lines="warn", encoding="utf-8", engine="python")

        except UnicodeDecodeError:
            data_frame = pd.read_csv(src_file, sep=";", on_bad_lines="warn", encoding="cp1252", engine="python")

    else:
        data_frame = pd.read_excel(src_file)

    column_names = data_frame.columns
    number = data_frame[column_names[0]].tolist()
    print(type(number[0]) == int)
    if "№" in str(number[0]):
        number = [str(_.split()[-1]) for _ in number]
    else:
        number = [str(_) for _ in number]
    print(type(number[0]))

    return number


def reestr_numbers():
    """Получаем номера аукционов со страницы"""

    # links = []
    start = time.time()
    number_list = []
    for pageNumber in range(1, 101):
        page_number = pars(pageNumber)
        if page_number is not None:
            soup = bs(page_number.content, "html.parser")
            number = soup.findAll("div", {"class": "registry-entry__header-mid__number"})
            print(len(number))
            for _ in number:
                link = _.find("a").get("href")
                reestr_number = link.split("=")[-1]
                number_list.append(reestr_number)
                # links.append
        else:
            number_list = []

        print(len(number_list))
        # time.sleep(0.1)

    print(f"С сайта {type(number_list[0])} за {int(time.time() - start)} сек.")
    return number_list


def press(button):
    """ Выполняет нажатие кнопки

    Аргументы:
        button: название кнопки. Используем названия Выполнить или Выход
    """
    start = time.time()
    # print(app.getCheckBox("Добавить к имени файла текущую дату"))

    if button == "Старт":
        src_file = app.getEntry("Input_File")
        errors, error_msg, extension = check_file(src_file)
        if errors:
            app.errorBox("Ошибка", "\n".join(error_msg), parent=None)
        else:
            auction_site = reestr_numbers()
            auction_source = auction_numbers(src_file, extension)
            print(f"Собрано: {len(auction_site)}\nВ базе: {len(auction_source)}")
            counter = Counter(auction_site)
            print(counter)
            auction_site_set = set(auction_site)
            auction_source_set = set(auction_source)
            print(f"Уникально собрано с сайта: {len(auction_site_set)}")
            auctions = auction_site_set - auction_source_set
            pprint(auctions)
            # print(f"Уникальных: {len(auctions)}")

        info(auctions)

        save_dir = app.getEntry("Output_Directory")
        write(save_dir)

        print(f"Выполнено за {round((time.time() - start), 2)} секунд")

    else:
        app.stop()


# Создать окно пользовательского интерфейса
app = gui(f"Закупки | {current_date}", useTtk=True)
app.setTtkTheme("alt")
app.setSize(500, 200)

# Добавить интерактивные компоненты
app.addLabel("Выберите исходную базу")
app.addFileEntry("Input_File")

app.addLabel("Выберите папку для сохранения")
app.addDirectoryEntry("Output_Directory")

# app.addLabel("Page Ranges: 1,3,4-10")
# app.addEntry("Page_Ranges")
cd = app.addCheckBox("Добавить к имени файла текущую дату")

# Связать кнопки с функцией под названием press
app.addButtons(["Старт"], press)


if __name__ == "__main__":
    app.go()
