import os
if not os.path.exists('logs'):
    os.mkdir('logs')

from functions import delete_downloaded_and_old_files
import time
from dotenv import load_dotenv
from psql import get_queued_posts, get_gives_information_status
import logging
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

handler_console = logging.StreamHandler()
handler_console.setFormatter(formatter)
# logger.addHandler(handler_console)

basedir = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('.env'):
    load_dotenv(os.path.join(basedir, '.env'))
else:
    load_dotenv(os.path.join(basedir, '.envexample'))


while True:

    delete_downloaded_and_old_files()

    df = get_queued_posts()
    if df.empty:
        time.sleep(1)
        continue

    queue_dict = df[df['timestamp'] == min(df['timestamp'])].iloc[0].to_dict()

    module = __import__('processings')
    func_to_execute = getattr(module, queue_dict['form_name'])

    gives_information_status = get_gives_information_status('audit._sales__execution_orders')

    if (gives_information_status['gives_information'] or
        (not gives_information_status['gives_information'] and
         not gives_information_status['wants_to_refresh'])):
        func_to_execute(post_id=queue_dict['id'])
