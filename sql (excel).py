import os, sys
from dotenv import load_dotenv
import pandas as pd
from pandas import ExcelWriter
import psycopg2
from dwh import dwh_connection
from datetime import datetime
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from psql import set_task_progress
from colorama import Fore
from config import logger

basedir = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('.env'):
    load_dotenv(os.path.join(basedir, '.env'))
else:
    load_dotenv(os.path.join(basedir, '.envexample'))
    basedir = os.path.abspath(os.path.dirname(__file__))
    
chunk_size= host=os.environ.get('CHUNK_SIZE')

psql_connection=psycopg2.connect(host=os.environ.get('POSTGRESQL_HOST'),
                               port = os.environ.get('POSTGRESQL_PORT'),
                               database=os.environ.get('POSTGRESQL_DBNAME'),
                               user=os.environ.get('POSTGRESQL_USER'),
                               password=os.environ.get('POSTGRESQL_PWD'))

dwh_connection=Client(host=os.environ.get('CLICK_HOST'),
                  port = os.environ.get('CLICK_PORT'),
                  database=os.environ.get('CLICK_DBNAME'),
                  user=os.environ.get('CLICK_USER'),
                  password=os.environ.get('CLICK_PWD'),
                  secure=True,
                  verify=False)


chunk_size = int(os.environ.get('CHUNK_SIZE'))


def DownloadForStationForm(post_id, file_name, **kwargs):

    print(Fore.CYAN, f"chunk_size: {chunk_size}", Fore.WHITE)
    logger.info(f'Запущено: DownloadForStationForm')
    logger.info(f'post_id: {post_id}')
    logger.info(f'file_name: {file_name}')
    logger.info(f'CHUNK_SIZE: {chunk_size}')
    rows_qty_sql = f"""SELECT
                        count(*)
                    FROM 
                        audit._sales__execution_orders
                    WHERE 
                        `Ст. Отправления` = '{kwargs["station_name"]}' AND 
                        `SVOD.Дата отправки` BETWEEN '{kwargs["start_date"]}' AND '{kwargs["finish_date"]}'"""                  
                        
    try:                    
        with dwh_connection as conn:
            rows_qty = conn.execute(rows_qty_sql)[0][0]
            logger.info(f'rows_qty: {rows_qty}')
    except Exception as e:
        logger.error('Не удалось получить общее число строк выгрузки') 
        return   
             
    try:     
        with ExcelWriter(file_name, mode='w') as writer:
            df = dwh_connection.query_dataframe("SELECT TOP(0) * FROM audit._sales__execution_orders")
            df.to_excel(writer, index=False)
            logger.info(f'Создан пустой файл {file_name}')             
    except Exception as e:    
        logger.error(f'Не удалось создать пустой файл {file_name}')
        return

    # вычисляем последнее удачное смещение на случай, если приходится возобновить загрузку    
    if 'загружено' in kwargs['task_status']:
        start_upset=int(kwargs['task_status'].split(' ')[1])
        logger.info(f'возовбновляем после перезапуска queue_manager')
    else:
        start_upset=0
        logger.info(f'первый запуск')

    for upset in range (start_upset, rows_qty, chunk_size): 

        logger.info(f"запущено для смещения {upset}")
        print(f"{datetime.now()} rows_qty {rows_qty} chunk_size {chunk_size} смещение {upset}")          

        try:
            with dwh_connection as conn:
                df = dwh_connection.query_dataframe(sql)

            with ExcelWriter(file_name, mode='a', if_sheet_exists='overlay') as writer:                   
                sql = f"""SELECT 
                                *
                            FROM 
                                audit._sales__execution_orders
                            WHERE 
                                `Ст. Отправления` = '{kwargs["station_name"]}' AND 
                                `SVOD.Дата отправки` BETWEEN '{kwargs["start_date"]}' AND '{kwargs["finish_date"]}'
                        ORDER BY
                            rownumber
                        LIMIT {chunk_size} OFFSET {upset} """
                

                print(sys.getsizeof(df)/1024/1024)
                df.to_excel(writer, startrow=upset+1, header=False, index=False)

            set_task_progress(post_id, f"загружено {upset} из {rows_qty}")
            print(f"{datetime.now()} rows_qty {rows_qty} chunk_size {chunk_size} смещение {upset}")    


        except Exception as e:
            logger.error(e)
            print(e)
            return
        
    try:
        set_task_progress(post_id, "файл подготовлен")
        logger.info("файл подготовлен")
    except Exception as e:
        logger.error(e)

                
def DownloadForRailwayForm(post_id, file_name, **kwargs):

    # вычисляем последнее удачное смещение на случай, если приходится возобновить загрузку    
    if 'загружено' in kwargs['task_status']:
        start_upset=int(kwargs['task_status'].split(' ')[1])
        rows_qty=int(kwargs['task_status'].split(' ')[3])
        logger.info(f'Возовбновляем после перезапуска (start_upset: {start_upset})')
    else:
        # print(Fore.RED,kwargs,Fore.WHITE)
        logger.info(f'Запущено: DownloadForRailwayForm')
        logger.info(f'post_id: {post_id}')
        logger.info(f'file_name: {file_name}')
        logger.info(f'CHUNK_SIZE: {chunk_size}')

        try:
            rows_qty_sql = f"""SELECT
                    count(*)
                FROM 
                    audit._sales__execution_orders
                WHERE 
                    `Дор. Отправления` = '{kwargs['conditions']["railway_name"]}' AND 
                    `SVOD.Дата отправки` BETWEEN '{kwargs['conditions']["start_date"]}' AND '{kwargs['conditions']["finish_date"]}'"""                  
            with dwh_connection as conn:
                rows_qty = conn.execute(rows_qty_sql)[0][0]
                logger.info(f'rows_qty: {rows_qty}')            
        except Exception as e:
            logger.error('Не удалось получить общее число строк выгрузки') 
            return    
         
        try:     
            with ExcelWriter(file_name, mode='w') as writer:
                df = dwh_connection.query_dataframe("SELECT TOP(0) * FROM audit._sales__execution_orders")
                df.to_excel(writer, index=False)
                logger.info(f'Создан пустой файл {file_name}') 
            start_upset=0
            logger.info(f'первый запуск')            
        except Exception as e:    
            logger.error(f'Не удалось создать пустой файл {file_name}')
            return


    for upset in range (start_upset, rows_qty, chunk_size):  
        logger.info(f"запущено для смещения {upset}")
        print(f"{datetime.now()} rows_qty {rows_qty} chunk_size {chunk_size}. Запущено для смещения {upset}")
        try:
            with ExcelWriter(file_name, mode='a', if_sheet_exists='overlay') as writer:                   
                sql = f"""SELECT 
                                *
                            FROM 
                                audit._sales__execution_orders
                            WHERE 
                                `Дор. Отправления` = '{kwargs['conditions']["railway_name"]}' AND 
                                `SVOD.Дата отправки` BETWEEN '{kwargs['conditions']["start_date"]}' AND '{kwargs['conditions']["finish_date"]}'
                        ORDER BY
                            rownumber
                        LIMIT {chunk_size} OFFSET {upset} """
                with dwh_connection as conn:
                    df = dwh_connection.query_dataframe(sql)
                    print(sys.getsizeof(df)/1024/1024)
                    #print(sys.getsizeof(writer))
                    df.to_excel(writer, startrow=upset+1, header=False, index=False)
                    #print(sys.getsizeof(writer))

                logger.info(f"успешно для смещения {upset}")
                set_task_progress(post_id, f"загружено {upset} из {rows_qty}")                    

        except Exception as e:
            logger.error(e)
            print(e)
            return

    try:
        set_task_progress(post_id, "файл подготовлен")
        logger.info("файл подготовлен")
    except Exception as e:
        logger.error(e)



if __name__ == '__main__':
    DownloadForStationForm(1,file_name='bbb.xlsx',
                                   station_name='Находка-Восточная (эксп.)',
                                   start_date='2022-01-01',
                                   finish_date='2024-07-13')