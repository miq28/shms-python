import os
import logging
import logging.config
# from pythonjsonlogger import jsonlogger

# logger = logging.getLogger(__name__)
# logger = logging.getLogger(os.path.basename(__file__))

# load config from file
# logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
# or, for dictConfig
MY_LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,  # this fixes the problem
    'formatters': {
        'simple': {
            # 'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s - (%(filename)s:%(lineno)d)',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s - (line:%(lineno)d)',
            'datefmt': "%Y-%m-%d %H:%M:%S%z",
        },
        # "json": {
        #     "format": "%(asctime)s %(levelname)s %(message)s",
        #     "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        # }
    },
    'handlers': {
        'default': {
            'level':'INFO',
            'class':'logging.StreamHandler',
            "formatter": 'simple',
            "stream": "ext://sys.stdout",
        },
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        },
        "stdout": {
            'level':'INFO',
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "simple",
        },
        "stderr": {
            'level':'WARNING',
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "formatter": "simple",
        },
        "debug_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            'mode':'w',
            "level": "DEBUG",
            "formatter": "simple",
            "filename": "debug.log",
            "maxBytes": 10485760,
            "backupCount": 0,
            "encoding": "utf8"
        },
        "info_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "simple",
            "filename": "info.log",
            "maxBytes": 10485760,
            "backupCount": 0,
            "encoding": "utf8"
        },
        "error_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",
            "formatter": "simple",
            "filename": "errors.log",
            "maxBytes": 10485760,
            "backupCount": 0,
            "encoding": "utf8"
        }        
    },
    'loggers': {
        '': {
            # 'handlers': ['default'],
            'handlers': ['stdout','stderr',"debug_file_handler","info_file_handler", "error_file_handler"],
            'level': 'DEBUG',
            'propagate': False
        }
    },
    # "root": {
    #     "level": "DEBUG",
    #     "handlers": ['console',"debug_file_handler","info_file_handler", "error_file_handler"]
    # }
}

logging.config.dictConfig(MY_LOG_CONFIG)