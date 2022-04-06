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


current_date = time.strftime("%d-%m-%Y")


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


URL = "https://zakupki.gov.ru/epz/contract/search/results.html?morphology=on&search-filter=Дате+размещения&fz44=on&contractStageList_0=on&contractStageList_1=on&contractStageList_2=on&contractStageList_3=on&contractStageList=0%2C1%2C2%2C3&contractCurrencyID=-1&budgetLevelsIdNameHidden=%7B%7D&publishDateFrom=19.07.2021&sortBy=UPDATE_DATE&pageNumber=1&sortDirection=false&recordsPerPage=_100&showLotsInfoHidden=false"

HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0',
           'accept': '*/*'
           }


def pars():

    response = requests.get(url=URL, headers=HEADERS, timeout=3)
    return response


def reesstrnumbers():

    blanks = []
    reestr_numbers = []
    soup = bs(pars().content, "html.parser")
    blank = soup.findAll("div", {"class": "registry-entry__header-mid__number"})
    for _ in blank:
        link = _.find("a").get("href")
        reestr_number = link.split("=")[-1]
        reestr_numbers.append(reestr_number)
        blanks.append(link)
    return reestr_numbers


info_table = []


def info():
    r_num = reesstrnumbers()
    url = f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber="

    for num in tqdm(r_num):
        responce = requests.get(url=f"{url}{num}", headers=HEADERS, timeout=3)
        soup = bs(responce.content, "html.parser")
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

        time.sleep(0.404)
    return info_table


def write():
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
            book.save(f'zakupki-{current_date}.xlsx')
            book.close()
            break

        except PermissionError:
            print('Кажется у Вас открыт файл в Exel,\nПожалуйста, закройте его, и тогда я смогу сохранить.')
            input('Закройте Exel, затем вернитесь сюда и нажмите ENTER')


def press(button):
    """ Выполняет нажатие кнопки

    Аргументы:
        button: название кнопки. Используем названия Выполнить или Выход
    """
    print(app.getCheckBox("Добавить к имени файла текущую дату"))

    if button == "Старт":
        src_file = app.getEntry("Input_File")
        
        print(src_file)
        save_dir = app.getEntry("Output_Directory")
        # out_file = app.getEntry("Output_name")
        # errors, error_msg = validate_inputs(src_file, save_dir, out_file)
        # if errors:
        #     app.errorBox("Error", "\n".join(error_msg), parent=None)
        # else:
        #     split_pages(src_file, page_range, Path(dest_dir, out_file))
    # else:
    #     app.stop()


# Создать окно пользовательского интерфейса
app = gui(f"Закупки | {current_date}", useTtk=True)
app.setTtkTheme("default")
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

# Запуск интерфейса
# app.go()


if __name__ == "__main__":
    app.go()
    # start = time.time()
    # pprint(info())
    # write()
    # print(round((time.time() - start), 2))
