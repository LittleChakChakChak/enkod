import pandas as pd
import datetime
import numpy as np
import smtplib

print(f'Старт скрипта: {datetime.datetime.now()}')

# пути к файлам
FileDepSQL = './enkod_dep.csv'   # файл с депами
FileBetsSQL = './enkod_bets.csv' # файл со ставками
FileEnkod = './enkod.csv'                # файл с данными enkod
FileDepEnkod = './dep_click.csv'     # данные с enkod (дата клика) + данные с dep
FileFinal = './enkod_summdep.csv'                # финальный файл enkod + dep

# для записи депов подходящие по дате клика (финальный df)
df_dep_click = pd.DataFrame([],
            columns=['ID_CLIENT', 'SUMDEP', 'DATEDEP', 'email', 'dep', 'action',
                     'dateClick', 'messageId', 'deltaDate'])
# для записи ставок подходящие по дате клика (финальный df)
df_bets_click = pd.DataFrame([],
            columns=['ID_CLIENT', 'SUMBET', 'DATEBET', 'email', 'bet', 'action',
                     'dateClick', 'messageId', 'deltaDate'])
# df_dep_click.to_csv(FileTest, index=False)

# формат даты и времени
date_format = "%Y-%m-%d"
datetime_format = "%Y-%m-%d %X" # с временим

# Уведомление на почту
def mail(flag = False):
    # От кого:
    fromaddr = 'адрес почты тот кто отправил (в письме)'

    # Кому:
    toaddr = 'адрес почты кому придет'

    #Тема письма:
    subj = f'enkod + dep - {flag} - in {str(datetime.datetime.now())}'

    #Текст сообщения:
    msg_txt = f'{flag} - enkod + dep in file in {str(datetime.datetime.now())}'

    #Создаем письмо (заголовки и текст)
    msg = "From: %s\nTo: %s\nSubject: %s\n\n%s" % (fromaddr, toaddr, subj, msg_txt)

    # Логин gmail аккаунта. Пишем только имя ящика
    username = 'адрес почты тот кто отправил'

    # Соответственно, пароль от ящика:
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

try:
     # получение df
     df_dep = pd.read_csv(FileDepSQL)
     df_bets = pd.read_csv(FileBetsSQL)
     df = pd.read_csv(FileEnkod, low_memory=False)

     # поиск взаимосвязей по дате
     # фильтрыем, сортируем, убираем дубли данные с enkod по кликам
     df['dateClick'] = df['dateClick'].astype('datetime64[ns]')

     df_filter_click = df.query('click == 1')
     df_filter_click = df_filter_click.sort_values(by=['email', 'dateClick'])

     df_dep['DATEDEP'] = df_dep['DATEDEP'].astype('datetime64[ns]')
     df_bets['DATEBETS'] = df_bets['DATEBETS'].astype('datetime64[ns]')

     print(f'Поиск соотношений: {datetime.datetime.now()}')

     # DEP -------------------------------------------------------------------------------------------------------
     for line_dep in df_dep.itertuples():

          # ищем соответствие по email в enkod (отрезая другие данные df)
          df_filter_email = df_filter_click.query('email == @line_dep.email & dateClick <= @line_dep.DATEDEP')

          # пустое значение запроса по enkod
          if not df_filter_email.empty:

              # сортираем дату по убыванию
              df_filter_date = df_filter_email.sort_values(by='dateClick', ascending=False)

              # пробегаем по отсортированным данным enkod
              for line_filter_date in df_filter_date.itertuples():
                  # ищем соответствие по ближайщей дате
                  if line_dep.DATEDEP >= line_filter_date.dateClick:
                      columns = list(line_dep._fields[1:])
                      data = list(line_dep[1:])
                      df_time = pd.DataFrame([data], columns=columns)
                      df_time['dateClick'] = line_filter_date.dateClick
                      df_time['messageId'] = line_filter_date.messageId
                      # подсчет дельты времени ()
                      deltaDate = line_dep.DATEDEP - line_filter_date.dateClick
                      days, seconds = deltaDate.days, deltaDate.seconds
                      deltaDate = (days * 24) + (seconds / 3600)
                      df_time['deltaDate'] = deltaDate

                      df_dep_click = pd.concat([df_dep_click, df_time])

                      break

     # BETS -------------------------------------------------------------------------------------------------------

     for line_bets in df_bets.itertuples():

         # ищем соответствие по email в enkod (отрезая другие данные df)
         df_filter_email = df_filter_click.query('email == @line_bets.email & dateClick <= @line_bets.DATEBETS')

         # пустое значение запроса по enkod
         if not df_filter_email.empty:

             # сортираем дату по убыванию
             df_filter_date = df_filter_email.sort_values(by='dateClick', ascending=False)

             # пробегаем по отсортированным данным enkod
             for line_filter_date in df_filter_date.itertuples():
                 # ищем соответствие по ближайщей дате
                 if line_bets.DATEBETS >= line_filter_date.dateClick:
                     columns = list(line_bets._fields[1:])
                     data = list(line_bets[1:])
                     df_time = pd.DataFrame([data], columns=columns)
                     df_time['dateClick'] = line_filter_date.dateClick
                     df_time['messageId'] = line_filter_date.messageId
                     # подсчет дельты времени ()
                     deltaDate = line_bets.DATEBETS - line_filter_date.dateClick
                     days, seconds = deltaDate.days, deltaDate.seconds
                     deltaDate = (days * 24) + (seconds / 3600)
                     df_time['deltaDate'] = deltaDate

                     df_bets_click = pd.concat([df_dep_click, df_time])

                     break

     # -------------------------------------------------------------------------------------------------------------

     # записываем депы с датами клика
     df_dep_click.to_csv(FileDepEnkod, index=False)

     # объединение df
     # df_dep_click = pd.read_csv(FileDepEnkod)
     # df_bets_click = pd.read_csv(FileBetsEnkod)
     # df_dep_click['dateClick'] = df_dep_click['dateClick'].astype('datetime64[ns]')
     # df_bets_click['dateClick'] = df_bets_click['dateClick'].astype('datetime64[ns]')
     new_df = pd.concat([df, df_dep_click])
     new_df = pd.concat([new_df, df_bets_click])

     # удаление дублей по активности
     new_df.loc[new_df['action'] == 'click', 'double'] = new_df[new_df['action'] == 'click'].duplicated(subset=['email',
                                                                                'dateClick', 'messageId'], keep='first')
     new_df.loc[new_df['action'] == 'open', 'double'] = new_df[new_df['action'] == 'open'].duplicated(subset=['email',
                                                                                'dateOpen', 'messageId'], keep='first')
     new_df.loc[new_df['action'] == 'send', 'double'] = new_df[new_df['action'] == 'send'].duplicated(subset=['email',
                                                                                'dateSend', 'messageId'], keep='first')
     new_df.loc[new_df['action'] == 'complaint', 'double'] = new_df[new_df['action']
                        == 'complaint'].duplicated(subset=['email', 'dateComplaint', 'messageId'], keep='first')
     new_df.loc[new_df['action'] == 'unsubscribe', 'double'] = new_df[new_df['action']
                        == 'unsubscribe'].duplicated(subset=['email', 'dateUnsubscribe', 'messageId'], keep='first')

     # new_df['double'] = new_df.query("double == True & double == False").astype(int)
     new_df.loc[new_df['double'] == np.nan, 'double'] = ''
     new_df.loc[new_df['double'] == True, 'double'] = '0'
     new_df.loc[new_df['double'] == False, 'double'] = '1'

     # проставление тригеров по ссылки на все id письма
     messageIds = pd.unique(new_df['messageId'])

     for messageId in messageIds:
         try:
             lang = new_df.query("messageId == @messageId & action == 'click'")['lang'].values[0]
             new_df.loc[new_df['messageId'] == messageId, 'lang'] = lang
         except:
             new_df.loc[new_df['messageId'] == messageId, 'lang'] = ''

     new_df.to_csv(FileFinal, index=False)
     mail(True)
except:
     mail(False)

print(f'Happy and!: {datetime.datetime.now()}')