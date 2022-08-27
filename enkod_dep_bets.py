import pandas as pd
import cx_Oracle
import time
import smtplib
import datetime

# данные по подключению
ConnectStr = """(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(Host=host)
                   (Port=1521))(CONNECT_DATA=(SERVICE_NAME=name)))"""

# данные пользователя
Login = 'secret'

# Уведомление на почту
def mail(meaning = 0, flag = False):
    # От кого:
    fromaddr = 'адрес почты тот кто отправил (в письме)'

    # Кому:
    toaddr = 'адрес почты кому придет'

    #Тема письма:
    subj = f'SQL(enkod) - {flag} - {meaning} in {str(datetime.datetime.now())}'

    #Текст сообщения:
    msg_txt = f'{flag} - loading {meaning} in file in {str(datetime.datetime.now())}'

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

# соединение к БД
def getConn(Login, ConnectStr):
    conn = None
    nn = 0
    while conn == None:
        try:
            conn = cx_Oracle.connect(Login + '@' + ConnectStr)
            print('Успешное подключение к SQL')
        except cx_Oracle.DatabaseError as e:
            ers, = e.args
            nn = nn + 1
            print(nn, end='\r')
            if ers.code != 2391:
                print('Ошибка Oracle ', ers.code)
                break
            time.sleep(5)
    return conn

# полючение данных
def dfFromOracle(connection, sql):
    outDF = pd.DataFrame()
    success = 'False'
    with connection.cursor() as cursor1:
        cursor1.execute(sql)
        trn = 10
        while success == 'False' and trn > 0:
            try:
                header = [desc[0] for desc in cursor1.description]
                # При вызове "cursor1.fetchall()" возвращается список записей, каждая из которых
                # является кортежем (неизменяемым списком) полей разного типа
                outDF = pd.DataFrame(cursor1.fetchall(), columns=header)
                success = 'True'
                print('Результат получен из базы')
            except:
                trn = trn - 1
                print('Error')
                time.sleep(60)
                input()
    return outDF


connection = getConn(Login, ConnectStr)

try:
    #Сформированный SQL-запрос по dep
    sql_dep = ("Select")

    outDF = dfFromOracle(connection, sql_dep)

    outDF.rename(columns={'EMAIL': 'email'}, inplace=True)
    outDF['dep'] = 1
    outDF['action'] = 'dep'

    outDF.to_csv('./enkod_dep.csv', index=False)

    mail('dep', True)
    print('dep сформированы')
except:
    mail(0, False)

try:
    #Сформированный SQL-запрос по bets
    sql_bets = ("SELECT")

    outDF = dfFromOracle(connection, sql_bets)

    outDF.rename(columns={'EMAIL': 'email'}, inplace=True)
    outDF['bet'] = 1
    outDF['action'] = 'bet'

    outDF.to_csv('./enkod_bets.csv', index=False)

    mail('bets', True)
    print('bets сформированы')
except:
    mail(0, False)