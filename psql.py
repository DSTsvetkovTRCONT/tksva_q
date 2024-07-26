import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import logging
from logging.handlers import TimedRotatingFileHandler

if not os.path.exists('logs'):
    os.mkdir('logs')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter_psql = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler_file_psql = TimedRotatingFileHandler("logs/tksva_q.log",
                                             encoding='utf-8',
                                             when='d',
                                             interval=1,
                                             backupCount=7)

handler_file_psql.setFormatter(formatter_psql)
logger.addHandler(handler_file_psql)

basedir = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('.env'):
    load_dotenv(os.path.join(basedir, '.env'))
else:
    load_dotenv(os.path.join(basedir, '.envexample'))

chunk_size = os.environ.get('CHUNK_SIZE')
how_many_hours_to_store = os.environ.get('HOW_MANY_HOURS_TO_STORE')


def get_queued_posts():
    logger.debug("Запущено: get_queued_posts. "
                 "Получаем список постов поставленных в очередь")

    try:
        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))
        sql = """SELECT
                    *
                FROM
                    post
                WHERE
                    task_status='в очереди' OR
                    task_status LIKE '%загружено % из %'"""

        with psql_connection:
            cur = psql_connection.cursor()
            cur.execute(sql)
            df = pd.DataFrame(cur.fetchall(),
                              columns=['id',
                                       'user_id',
                                       'form_name',
                                       'conditions',
                                       'task_status',
                                       'file_to_download',
                                       'task_status_timestamp',
                                       'timestamp'])
        return df
    except Exception:
        logger.exception("Не удалось получить список постов в очереди")


def get_downloaded_and_old_files():
    logger.debug("Запущено: get_downloaded_and_old_files. "
                 "Получаем список загруженных или старых файлов")

    try:
        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))

        with psql_connection:
            sql = f"""SELECT
                        id,
                        user_id,
                        task_status,
                        file_to_download
                    FROM
                        post
                    WHERE
                        task_status = 'файл загружен' OR
                        (task_status_timestamp + '{how_many_hours_to_store}' < now()
                        AND task_status NOT LIKE '%скрыта%')"""
            cur = psql_connection.cursor()
            cur.execute(sql)
            return pd.DataFrame(cur.fetchall(),
                                columns=['post_id', 'user_id', 'task_status', 'file_to_download'])

    except Exception:
        logger.exception("Не удалось получить список загруженных файлов")


def set_gives_information_status(dwh_table_name, status):
    logger.info(f"Запущено: set_dwh_tables_info_status. "
                f"Прописываем для {dwh_table_name} статус {status}")

    try:
        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))
        with psql_connection:
            cur = psql_connection.cursor()
            cur.execute(f"""UPDATE
                            dwh_tables_info
                        SET
                            gives_information = {status},
                            gives_information_timestamp = now()
                        WHERE
                            dwh_table_name = '{dwh_table_name}'""")

        return True
    except Exception:
        logger.exception("Не удалось прописать статус")
        return False


def get_gives_information_status(dwh_table_name):
    logger.info(f"Запущено: get_gives_information_status. "
                f"Получаем статусы для {dwh_table_name}")

    try:
        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))

        with psql_connection:
            cur = psql_connection.cursor()
            cur.execute(f"""SELECT
                                wants_to_refresh,
                                gives_information
                        FROM
                            dwh_tables_info
                        WHERE
                            dwh_table_name = '{dwh_table_name}'""")
            res_list = cur.fetchone()
        res_dict = {'wants_to_refresh': res_list[0],
                    'gives_information': res_list[1]}
        return res_dict
    except Exception:
        logger.exception("Не удалось получить статусы")
        return False


def set_task_status(post_id, task_status):
    logger.info(f"Запущено: set_task_status. "
                f"Прописываем статус для {post_id} - {task_status}")

    try:
        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))

        with psql_connection:
            cur = psql_connection.cursor()
            cur.execute(f"""UPDATE
                                post
                        SET
                            task_status ='{task_status}',
                            task_status_timestamp = now()
                        WHERE
                            id = {post_id}""")
    except Exception:
        logger.exception("Не удалось прописать статус")
        return False
    return True


def get_post(post_id):
    logger.info('Запущено: get_post. Получаем информацию о посте')

    try:
        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))

        sql = f"""SELECT * FROM post WHERE id={post_id}"""
        with psql_connection:
            cur = psql_connection.cursor()
            cur.execute(sql)
            post_list = cur.fetchone()

        return {'user_id': post_list[1],
                'conditions': post_list[3],
                'task_status': post_list[4],
                'file_to_download': post_list[5]}
    except Exception:
        logger.exception("Не удалось получить информацию о посте")


if __name__ == '__main__':
    print(get_gives_information_status('audit._sales__execution_orders'))
