
from os import getenv
from logging import DEBUG, INFO


API_VERSION = getenv('API_VERSION', '0.7.0')

BASE_URI = getenv('BASE_URI', 'http://www2.dgi.inpe.br/inpe-stac/')

FLASK_ENV = getenv('FLASK_ENV', 'production')

INPE_STAC_DELETED = getenv('INPE_STAC_DELETED', '0')

# default logging level in production server
LOGGING_LEVEL = INFO

# if the application is in development mode, then change the logging level and debug mode
if FLASK_ENV == 'development':
    LOGGING_LEVEL = DEBUG
