import psutil
import os, sys
from os import listdir, remove
import csv, sqlite3
import datetime, time

import threading as th
from statistics import mean
import multiprocessing




class task:

    def __init__(self, report_name = 'report.db', base_db = 'cheaters.db'):
        self.my_pid = os.getpid()
        self.memory = set()
        #self.memory.add(psutil.virtual_memory())
        self.report_name = report_name
        self.base_db = base_db
        self.locker = th.Lock()
        self.for_end_app = []




    def create_db(self, delete_exists = True):
        if self.report_name in listdir():
            if delete_exists:
                os.remove(self.report_name)
                self.create_db()
            else:
                pass
        else:
            connect_for_create = sqlite3.connect(self.report_name)
            connect_for_create.execute('create table report (timestamp, player_id, event_id, error_id, json_server, json_client)')
            connect_for_create.commit()
            connect_for_create.close()

    def in_datetime(self, date_time, onlydate = True, interrup_date_time = ' ', interrup_date = '-', interrup_time = ':'):
        if type(date_time) == str:
            Date = date_time.split(interrup_date_time)[0]
            try:
                Time = date_time.split(interrup_date_time)[1]
            except:
                Time = None
            try:
                Date = Date.split(interrup_date)
                if Time:
                    Time = Time.split(interrup_time)
                    if onlydate:
                        return datetime.datetime(int(Date[0]), int(Date[1]), int(Date[2]), int(Time[0]), int(Time[1]), int(Time[2])).date()
                    else:
                        return datetime.datetime(int(Date[0]), int(Date[1]), int(Date[2]), int(Time[0]), int(Time[1]), int(Time[2])).date()
                else:
                    return datetime.date(int(Date[0]), int(Date[1]), int(Date[2]))
            except Exception as e:
                print(f'Передан не допустимый формат даты и времени.\n'
                      f'Формат должен соответствовать => число{interrup_date}месяц{interrup_date}год{interrup_date_time}час{interrup_time}минут{interrup_time}секунд\n'
                      f'Либо => число{interrup_date}месяц{interrup_date}год{interrup_date_time}\n'
                      f'Пример корректного значения => 2023{interrup_date}09{interrup_date}23{interrup_date_time}12{interrup_time}00{interrup_time}37')
        elif type(date_time) == int or type(date_time) == float:
            try:
                row_time = time.gmtime(date_time)
                if onlydate:
                    return datetime.date(row_time.tm_year, row_time.tm_mon, row_time.tm_mday)
                else:
                    return datetime.date(row_time.tm_year, row_time.tm_mon, row_time.tm_mday, row_time.tm_hour, row_time.tm_min, row_time.tm_sec, row_time.tm_wday)
            except:
                print('Переданно недопустимое число')

    def work(self,  date_time, client='client.csv', server='server.csv'):

        with open(server) as cli:
            date_time = self.in_datetime(date_time)
            server_csv = csv.DictReader(cli)
            work_date = False

            print('Ожидайте')
            for  str_serv in server_csv:
                if date_time == self.in_datetime(int(str_serv["timestamp"])):

                    work_date = True
                    thread = th.Thread(target=self.for_thread, args = (client, str_serv))

                    self.for_end_app.append(thread)
                    thread.start()
                    self.memory.add(psutil.Process(self.my_pid).memory_info()[0])


            if not work_date:
                print('Для полученной даты нет событий')
            else:
                self.get_memory()


    def for_thread(self, client_name, str_serv):
        with open(client_name) as cli:
            client_csv = csv.DictReader(cli)
            for str_cli in client_csv:
                if str_serv['error_id'] == str_cli['error_id']:
                    self.locker.acquire()
                    #print('-------------------------')
                    connect_for_create = sqlite3.connect(self.base_db)
                    #print(str_cli['player_id'])
                    info_player = connect_for_create.execute('select * from cheaters where player_id = ?',(str_cli['player_id'],)).fetchone()
                    connect_for_create.close()
                    if info_player:
                        raz = self.in_datetime(int(str_serv['timestamp'])) - self.in_datetime(info_player[1])
                        if raz.days < 1:
                            #print('metka dly zapisi')
                            create_str_db = sqlite3.connect(self.report_name)
                            create_str_db.execute('insert into report (timestamp, player_id, event_id, error_id, json_server, json_client)\
                                                                        values (?,?,?,?,?,?)', (str_serv['timestamp'], str_cli['player_id'], str_serv['event_id'], str_cli['error_id'],str_serv['description'], str_cli['description']))
                            create_str_db.commit()
                            create_str_db.close()

                    #print('-------------------------')
                    self.locker.release()
                    break



    def get_memory(self):
        while any([i.is_alive() for i in self.for_end_app]):
            time.sleep(1)
            pass
        print(f'Максимальное потребление памяти => {mean(self.memory) / 1024 ** 2} Мбайт')
        input('Нажми "Enter" для выхода')


t = task()
t.create_db()

if __name__ == '__main__':
    try:
        if sys.argv[1]:
            t.work(sys.argv[1])
    except:
        choice_1 = input('Введите  дату, формат даты год-месяц-дата, пример даты => 2021-05-18\n')
        if choice_1:
            t.work(choice_1)
        else:
            choice_2 = input('Для запуска примера введите => y, yes  \n')
            if 'y' in choice_2:
                #Запуск примера
                t.work('2021-05-18')


