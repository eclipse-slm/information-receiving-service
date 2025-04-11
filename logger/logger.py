import logging

def set_basic_config():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def logger(LOG_LEVEL):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log = logging.getLogger(__name__)

    log.setLevel(LOG_LEVEL)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    log.addHandler(stream_handler)
    
    log.debug("Starting with log level: %s" % LOG_LEVEL)

    return log

LOG_LEVEL = 'DEBUG'
LOG = logger(LOG_LEVEL)

