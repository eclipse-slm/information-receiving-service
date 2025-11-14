import os
import signal
import threading
import time

from dotenv import load_dotenv

from logger.logger import LOG
from services import aas

load_dotenv()
crawl_interval = os.getenv("CRAWLER_INTERVAL_S")
endless_loop_active = False

def crawl():
    while endless_loop_active:
        aas.crawl_remote_aas_resources()
        LOG.info("Crawling done. Next crawl in %s seconds" % crawl_interval)
        time.sleep(int(crawl_interval))

crawling_thread = threading.Thread(target=crawl)

def start_stop_automatic_crawling() -> str:
    if endless_loop_active:
        stop_crawling()
        return "stopped"
    else:
        start_crawling()
        return "started"


def start_crawling():
    global endless_loop_active
    endless_loop_active = True
    crawling_thread.start()
    LOG.info("Crawling started with loop interval of %s seconds" % crawl_interval)

def stop_crawling():
    global endless_loop_active
    endless_loop_active = False
    crawling_thread.join()
    LOG.info("Crawling stopped")