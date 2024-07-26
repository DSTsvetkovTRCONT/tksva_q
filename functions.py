from psql import get_downloaded_and_old_files, set_task_status
import logging
import os
from logging.handlers import TimedRotatingFileHandler

if not os.path.exists('logs'):
    os.mkdir('logs')

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


def delete_downloaded_and_old_files():
    logging.info("Запускаем delete_downloaded_and_old_files. "
                 "Запускаем очистку загруженных или старых файлов")
    df = get_downloaded_and_old_files()
    if df.empty:
        logger.info("Загруженных или старых файлов не обнаружено")
        return
    else:
        try:
            for post in df.itertuples():
                file_to_download = (os.path.join("..",
                                                 "tksva",
                                                 "app",
                                                 "static",
                                                 "files_to_download",
                                                 str(post.user_id),
                                                 post.file_to_download))

                if os.path.exists(file_to_download):
                    os.remove(file_to_download)
                if post.task_status != "файл загружен":
                    set_task_status(post.post_id, "скрыта менеджером очереди")
        except Exception:
            logger.exception("Аварийно прервана зачистка "
                             "загруженных или старых файлов")


if __name__ == '__main__':
    delete_downloaded_and_old_files()
