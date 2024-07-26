import csv
import logging
import os
from dotenv import load_dotenv
from clickhouse_driver import Client
from psql import get_post, set_task_status, set_gives_information_status
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler_file = TimedRotatingFileHandler("logs/tksva_q.log",
                                        encoding='utf-8',
                                        when='d',
                                        interval=1,
                                        backupCount=7)
handler_file.setFormatter(formatter)
logger.addHandler(handler_file)

basedir = os.path.abspath(os.path.dirname(__file__))
# print(basedir)

if os.path.exists('.env'):
    load_dotenv(os.path.join(basedir, '.env'))
else:
    load_dotenv(os.path.join(basedir, '.envexample'))
    basedir = os.path.abspath(os.path.dirname(__file__))

chunk_size = int(os.environ.get('CHUNK_SIZE'))


def DownloadForStationForm(post_id):
    set_gives_information_status('audit._sales__execution_orders', True)
    # logger.info('*'*50)
    logger.info(f'Запущено: DownloadForStationForm для post: {post_id}')
    # logger.info('*'*50)

    post_dict = get_post(post_id)
    file_to_download = os.path.join(os.path.join('..', 'tksva', 'app', 'static', 'files_to_download', str(post_dict['user_id']), post_dict['file_to_download']))

    # вычисляем последнее удачное смещение на случай, если приходится возобновить загрузку
    if 'загружено' in post_dict['task_status']:
        start_upset = int(post_dict['task_status'].split(' ')[1]) + chunk_size
        rows_qty = int(post_dict['task_status'].split(' ')[3])
        logger.info(f'Возовбновляем после перезапуска (start_upset: {start_upset})')
    else:
        logger.info('Первый запуск')
        logger.info(f"file_name: {post_dict['file_to_download']}")
        logger.info(f'CHUNK_SIZE: {chunk_size}')

        try:
            logger.info('Получаем общее число строк выгрузки')
            rows_qty_sql = f"""SELECT
                    count(*)
                FROM
                    audit._sales__execution_orders
                WHERE
                    `Ст. Отправления` = '{post_dict['conditions']["station_name"]}' AND
                    `SVOD.Дата отправки` BETWEEN '{post_dict['conditions']["start_date"]}' AND
                    '{post_dict['conditions']["finish_date"]}'"""

            dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                    port=os.environ.get('CLICK_PORT'),
                                    database=os.environ.get('CLICK_DBNAME'),
                                    user=os.environ.get('CLICK_USER'),
                                    password=os.environ.get('CLICK_PWD'),
                                    secure=True,
                                    verify=False)

            with dwh_connection as conn:
                rows_qty = conn.execute(rows_qty_sql)[0][0]
            logger.info(f'rows_qty: {rows_qty}')
        except Exception:
            logger.exception('Не удалось получить общее число строк выгрузки')
            return False

        try:
            logger.info('Создаём файл, прописываем заголовки')
            dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                    port=os.environ.get('CLICK_PORT'),
                                    database=os.environ.get('CLICK_DBNAME'),
                                    user=os.environ.get('CLICK_USER'),
                                    password=os.environ.get('CLICK_PWD'),
                                    secure=True,
                                    verify=False)

            with dwh_connection as conn:
                df = dwh_connection.query_dataframe("SELECT TOP(0) * FROM audit._sales__execution_orders")
                column_names_list = df.columns[1:]

            with open(file_to_download,
                      mode='w',
                      encoding='utf-8',
                      newline='\n') as f:
                w = csv.writer(f, dialect='excel-tab', delimiter='\t')
                w.writerow(column_names_list)
            start_upset = 0

        except Exception:
            logger.exception("Не удалось создать пустой файл и "
                             "прописать заголовки")
            return False

    for upset in range(start_upset, rows_qty, chunk_size):
        try:
            logger.info(f"Получаем данные из DWH (смещение {upset})")
            sql = f"""SELECT
                            *
                        FROM
                            audit._sales__execution_orders
                        WHERE
                            `Ст. Отправления` = '{post_dict['conditions']["station_name"]}' AND
                            `SVOD.Дата отправки` BETWEEN '{post_dict['conditions']["start_date"]}' AND
                            '{post_dict['conditions']["finish_date"]}'
                    ORDER BY
                        rownumber
                    LIMIT {chunk_size} OFFSET {upset} """

            dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                    port=os.environ.get('CLICK_PORT'),
                                    database=os.environ.get('CLICK_DBNAME'),
                                    user=os.environ.get('CLICK_USER'),
                                    password=os.environ.get('CLICK_PWD'),
                                    secure=True,
                                    verify=False)

            with dwh_connection as conn:
                values_list = dwh_connection.execute(sql)
                values_list = [value[1:] for value in values_list]
        except Exception:
            logger.exception("Данные из DWH не получены")
            return False

        try:
            logger.info("Записываем в CSV")
            with open(file_to_download,
                      mode='a',
                      encoding='utf-8',
                      newline='\n') as f:

                w = csv.writer(f, dialect='excel-tab', delimiter='\t')
                w.writerows(values_list)
        except Exception:
            logger.exception("Ошибка записи в CSV")
            set_gives_information_status('audit._sales__execution_orders',
                                         False)
            set_task_status(post_id, "Ошибка записи в CSV")
            return False

        if not set_task_status(post_id, f"загружено {upset} из {rows_qty}"):
            return False
    if not set_task_status(post_id, "файл подготовлен"):
        return False

    set_gives_information_status('audit._sales__execution_orders', False)
    logger.info("файл подготовлен")
    return True


def DownloadForRailwayForm(post_id):
    set_gives_information_status('audit._sales__execution_orders', True)
    # logger.info('*'*50)
    logger.info(f'Запущено: DownloadForRailwayForm для post: {post_id}')
    # logger.info('*'*50)

    post_dict = get_post(post_id)
    file_to_download = os.path.join(os.path.join('..', 'tksva', 'app', 'static', 'files_to_download', str(post_dict['user_id']), post_dict['file_to_download']))

    # вычисляем последнее удачное смещение на случай, если приходится возобновить загрузку
    if 'загружено' in post_dict['task_status']:
        start_upset = int(post_dict['task_status'].split(' ')[1]) + chunk_size
        rows_qty = int(post_dict['task_status'].split(' ')[3])
        logger.info(f'Возовбновляем после перезапуска (start_upset: {start_upset})')
    else:
        logger.info('Первый запуск')
        logger.info(f"file_name: {post_dict['file_to_download']}")
        logger.info(f'CHUNK_SIZE: {chunk_size}')

        try:
            logger.info('Получаем общее число строк выгрузки')
            rows_qty_sql = f"""SELECT
                    count(*)
                FROM
                    audit._sales__execution_orders
                WHERE
                    `Дор. Отправления` = '{post_dict['conditions']["railway_name"]}' AND
                    `SVOD.Дата отправки` BETWEEN '{post_dict['conditions']["start_date"]}' AND
                    '{post_dict['conditions']["finish_date"]}'"""

            dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                    port=os.environ.get('CLICK_PORT'),
                                    database=os.environ.get('CLICK_DBNAME'),
                                    user=os.environ.get('CLICK_USER'),
                                    password=os.environ.get('CLICK_PWD'),
                                    secure=True,
                                    verify=False)

            with dwh_connection as conn:
                rows_qty = conn.execute(rows_qty_sql)[0][0]
            logger.info(f'rows_qty: {rows_qty}')
        except Exception:
            logger.exception('Не удалось получить общее число строк выгрузки')
            return False

        try:
            logger.info('Создаём файл, прописываем заголовки')
            dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                    port=os.environ.get('CLICK_PORT'),
                                    database=os.environ.get('CLICK_DBNAME'),
                                    user=os.environ.get('CLICK_USER'),
                                    password=os.environ.get('CLICK_PWD'),
                                    secure=True,
                                    verify=False)

            with dwh_connection as conn:
                df = dwh_connection.query_dataframe("""SELECT TOP(0)
                                                        *
                                                    FROM
                                                        audit._sales__execution_orders""")
                column_names_list = df.columns[1:]

            with open(file_to_download,
                      mode='w',
                      encoding='utf-8',
                      newline='\n') as f:
                w = csv.writer(f, dialect='excel-tab', delimiter='\t')
                w.writerow(column_names_list)

            logger.info("Файл создан, заголовки прописаны")
            start_upset = 0

        except Exception:
            logger.exception("Не удалось создать пустой файл "
                             "и прописать заголовки")
            return False

    for upset in range(start_upset, rows_qty, chunk_size):
        logger.info(f"запущено для смещения {upset}")
        try:
            logger.info("Получаем данные из DWH")
            sql = f"""SELECT
                            *
                        FROM
                            audit._sales__execution_orders
                        WHERE
                            `Дор. Отправления` = '{post_dict['conditions']["railway_name"]}' AND
                            `SVOD.Дата отправки` BETWEEN '{post_dict['conditions']["start_date"]}' AND
                            '{post_dict['conditions']["finish_date"]}'
                    ORDER BY
                        rownumber
                    LIMIT {chunk_size} OFFSET {upset} """

            dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                    port=os.environ.get('CLICK_PORT'),
                                    database=os.environ.get('CLICK_DBNAME'),
                                    user=os.environ.get('CLICK_USER'),
                                    password=os.environ.get('CLICK_PWD'),
                                    secure=True,
                                    verify=False)

            with dwh_connection as conn:
                values_list = dwh_connection.execute(sql)
                values_list = [value[1:] for value in values_list]
            logger.info("Данные из DWH получены")
        except Exception:
            logger.exception("Данные из DWH не получены")
            return False

        try:
            logger.info("Записываем в CSV")
            with open(file_to_download,
                      mode='a',
                      encoding='utf-8',
                      newline='\n') as f:
                w = csv.writer(f,
                               dialect='excel-tab',
                               delimiter='\t')
                w.writerows(values_list)
        except Exception:
            logger.exception("Ошибка записи в CSV")
            set_gives_information_status('audit._sales__execution_orders',
                                         False)
            set_task_status(post_id, "Ошибка записи в CSV")
            return False

        if not set_task_status(post_id, f"загружено {upset} из {rows_qty}"):
            return False

    if not set_task_status(post_id, "файл подготовлен"):
        return False

    set_gives_information_status('audit._sales__execution_orders', False)
    logger.info("файл подготовлен")
    return True


if __name__ == '__main__':
    print(basedir)
