import requests
import pandas as pd
import re
import datetime
from langdetect import detect
import aiohttp
import asyncio
import smtplib

# пути к файлам
FILE_WAY = './enkod.csv'
LOGS = './logs.txt'

# потоков
STREAMS = 5

# Уведомление на почту
def mail(page = 0, last_page = 0, flag = False):
    # От кого:
    fromaddr = 'адрес почты тот кто отправил (в письме)'

    # Кому:
    toaddr = 'адрес почты кому придет'

    #Тема письма:
    subj = f'Enkod - {flag} - Pages {page} from {last_page} in {str(datetime.datetime.now())}'

    #Текст сообщения:
    msg_txt = f'{flag} - loading enkod \n\n Pages worked out {page} from {last_page} in {str(datetime.datetime.now())}'

    #Создаем письмо (заголовки и текст)
    msg = "From: %s\nTo: %s\nSubject: %s\n\n%s" % (fromaddr, toaddr, subj, msg_txt)

    #Логин gmail аккаунта. Пишем только имя ящика
    username = 'адрес почты тот кто отправил'

    #Соответственно, пароль от ящика:
    password = ''

    #Инициализируем соединение с сервером gmail по протоколу smtp.
    server = smtplib.SMTP('smtp.gmail.com:587')

    #Выводим на консоль лог работы с сервером (для отладки)
    server.set_debuglevel(1)

    #Переводим соединение в защищенный режим (Transport Layer Security)
    server.starttls()

    # Проводим авторизацию:
    server.login(username, password)

    # Отправляем письмо:
    server.sendmail(fromaddr, toaddr, msg)

    # Закрываем соединение с сервером
    server.quit()

# для чистки ссылки
def linkParser(mark, url):
    result = re.search(r'{}=(.+?)(&|$)'.format(mark), url)
    if result:
        return result.group(1)
    return ''

def actionDate(action, date):
    # Распределение даты
    if action == 'click':
        click = 1
        send = ''
        open = ''
        complaint = ''
        unsubscribe = ''
        dateClick = date
        dateOpen = ''
        dateSend = ''
        dateComplaint = ''
        dateUnsubscribe = ''
    elif action == 'open':
        click = ''
        send = ''
        open = 1
        complaint = ''
        unsubscribe = ''
        dateClick = ''
        dateOpen = date
        dateSend = ''
        dateComplaint = ''
        dateUnsubscribe = ''
    elif action == 'send':
        click = ''
        send = 1
        open = ''
        complaint = ''
        unsubscribe = ''
        dateClick = ''
        dateOpen = ''
        dateSend = date
        dateComplaint = ''
        dateUnsubscribe = ''
    elif action == 'complaint':
        click = ''
        send = ''
        open = ''
        complaint = 1
        unsubscribe = ''
        dateClick = ''
        dateOpen = ''
        dateSend = ''
        dateComplaint = date
        dateUnsubscribe = ''
    elif action == 'unsubscribe':
        click = ''
        send = ''
        open = ''
        complaint = ''
        unsubscribe = 1
        dateClick = ''
        dateOpen = ''
        dateSend = ''
        dateComplaint = ''
        dateUnsubscribe = date

    return click, open, send, complaint, unsubscribe, dateClick, dateOpen, dateSend, dateComplaint, dateUnsubscribe

# получение статистики письма
async def getDataLetter(id):
    urlLetter = f'https://api.enkod.ru/v1/statistic/message/{id}'

    async with aiohttp.ClientSession() as session:
        async with session.get(urlLetter, headers=headers) as resp:
            dataLetter = await resp.json()

    name = dataLetter['name']
    # lang_name = detect(dataLetter['name'])

    return [name]#, lang_name]

# пробегаем каждую страницу
async def working_data(url, headers, params, columns, check, email, date, page, total_page):
    while page <= total_page:
        # изменение параметров запроса, добавление страницы
        params['page'] = page
        data = []
        # получаем данные постранично
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                list_active = await resp.json()

                try:
                    list_active = list_active['actions']
                except:
                    # запись в логи
                    with open(LOGS, 'a+') as file:
                        file.writelines(
                            f'Ошибка {list_active}в {str(datetime.datetime.now())}\n')

        # читаем каждую запись
        for record in list_active:
            if 'email' in record:

                # проверка последней записи в excel
                if record.get('email', '') != email_old and record.get('dateTime', '') != date_old and not check:
                    check = True
                    continue
                else:
                    if check:
                        # готовим структуру для красивой записи

                        action = record.get('action', '')
                        dateTime = record.get('dateTime', '')

                        click, open, send, complaint, unsubscribe, \
                        dateClick, dateOpen, dateSend, dateComplaint, dateUnsubscribe = actionDate(action, dateTime)

                        correct_data = [page, record.get('email', ''), record.get('messageId', ''),
                                        record.get('action', ''),

                                        # action
                                        click, open, send, complaint, unsubscribe,
                                        dateClick, dateOpen, dateSend, dateComplaint, dateUnsubscribe,

                                        record.get('additional', ''),
                                        record.get('channel', '')]

                        # распарсивание ссылки (additional)
                        additional = record.get('additional', '')

                        if additional != '':
                            lang = linkParser('lang', additional)
                            utm_source = linkParser('utm_source', additional)
                            utm_medium = linkParser('utm_medium', additional)
                            utm_campaign = linkParser('utm_campaign', additional)
                            utm_content = linkParser('utm_content', additional)
                            correct_data += [lang, utm_source, utm_medium, utm_campaign, utm_content]
                        else:
                            correct_data += ['', '', '', '', '']

                        # получение статистики письма
                        dataLetter = record.get('messageId', '')
                        if dataLetter != '':
                            correct_data += await getDataLetter(dataLetter)
                        else:
                            correct_data += ['']

                        # добавление записи в массив
                        data.append(correct_data)

        print(f'Страниц отработано {page} из {total_page} в {str(datetime.datetime.now())}\n')
        page += 1

        # новые данные
        df = pd.DataFrame(data, columns=columns)

        # запись в excel файл
        df.to_csv(FILE_WAY, index=False, mode='a', header=False)

        with open(LOGS, 'a+') as file:
           file.writelines(
               f'Страниц отработано {page-1} из {total_page} в {str(datetime.datetime.now())}\n')
           print('Файл заполнен')

try:
    # адрес api action
    url = "https://api.enkod.ru/v1/actions/ensend/all/?"
    headers = {'apiKey' : 'secret key'}
    params = {'channel': 'email', 'action': ['send', 'open', 'click', 'complaint', 'unsubscribe']}

    # колонки в файл
    columns = ['page', 'email', 'messageId', 'action', 'click', 'open', 'send', 'complaint', 'unsubscribe',
               'dateClick', 'dateOpen', 'dateSend', 'dateComplaint', 'dateUnsubscribe', 'additional',
               'channel', 'lang', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'name']

    page = 1
    check = False

    # отправляем общий запрос
    response = requests.get(url, headers=headers, params=params)
    # получаем данные
    list_active = response.json()

    # общее количество страниц
    total_page = list_active['totalPage']

    # просмотр последнией записи в excel
    email_old = ''
    date_old = ''

    try:
        df = pd.read_csv(FILE_WAY, low_memory=False)
        page = df.max().page
        df_tail = df[df['page'] == page]
        email_old = df_tail.tail(1).email.values[0]
        if str(df_tail.tail(1).dateClick.values[0]) != 'nan':
            date_old = df_tail.tail(1).dateClick.values[0]
        elif str(df_tail.tail(1).dateOpen.values[0]) != 'nan':
            date_old = df_tail.tail(1).dateOpen.values[0]
        elif str(df_tail.tail(1).dateSend.values[0]) != 'nan':
            date_old = df_tail.tail(1).dateSend.values[0]
        elif str(df_tail.tail(1).dateComplaint.values[0]) != 'nan':
            date_old = df_tail.tail(1).dateComplaint.values[0]
        elif str(df_tail.tail(1).dateUnsubscribe.values[0]) != 'nan':
            date_old = df_tail.tail(1).dateUnsubscribe.values[0]
    except:
        df = pd.DataFrame([], columns=columns)
        df.to_csv(FILE_WAY, index=False)
        check = True

    # ------------------------------------------------------------------------------------

    i = 1
    tasks = []
    min_page = page
    max_page = total_page


    sum_pages = max_page - min_page + 1
    period = int(sum_pages/STREAMS) # страниц за поток

    ioloop = asyncio.get_event_loop()

    if period > 0:
        # основное
        stack_1 = STREAMS * period # число четных загруженных
        while i <= STREAMS:
            max_page_task = min_page + period - 1
            ioloop = asyncio.get_event_loop()
            tasks.append(ioloop.create_task(working_data(url, headers, params, columns, check, email_old, date_old,
                                                         int(min_page), int(max_page_task))))
            min_page += period
            i += 1

        ioloop.run_until_complete(asyncio.wait(tasks))

        # дополнительно
        stack_2 = sum_pages - stack_1 # страницы, которые не загрузились в основлной куче
        i = 1
        while i <= stack_2:
            ioloop = asyncio.get_event_loop()
            tasks.append(
                ioloop.create_task(working_data(url, headers, params, columns, check, email_old, date_old,
                                                int(min_page), int(min_page))))
            min_page += 1
            i += 1

        ioloop.run_until_complete(asyncio.wait(tasks))
    else:
        # дозапись если меньше 20 страниц
        i = 1
        while i <= sum_pages:
            ioloop = asyncio.get_event_loop()
            tasks.append(
                ioloop.create_task(working_data(url, headers, params, columns, check, email_old, date_old,
                                                int(min_page), int(min_page))))
            min_page += 1
            i += 1

        ioloop.run_until_complete(asyncio.wait(tasks))

    print(f'Happy and {str(datetime.datetime.now())}')
    mail(page, max_page, True)
except:
    mail(page, max_page, False)