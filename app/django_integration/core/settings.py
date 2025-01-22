from pathlib import Path
from dotenv import dotenv_values

BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
MDB_POSTGRES_SETTINGS = dotenv_values(BASE_DIR.parent / 'secrets' / 'mdb-postgres.env')

# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'timescale.db.backends.postgis',
        #'ENGINE': 'django.db.backends.postgresql',
        'NAME': MDB_POSTGRES_SETTINGS['POSTGRES_DB'],
        'USER': MDB_POSTGRES_SETTINGS['POSTGRES_USER'],
        'PASSWORD': MDB_POSTGRES_SETTINGS['POSTGRES_PASSWORD'],
        'HOST': MDB_POSTGRES_SETTINGS['POSTGRES_HOST'],
        'PORT': MDB_POSTGRES_SETTINGS['POSTGRES_PORT'],
    }
}

INSTALLED_APPS = [
    'django.contrib.contenttypes', # Required for ORM
    'django.contrib.auth',         # Optional, if your models depend on it
    'django_integration.monitoring_db',               # Reference to the app for models.py
]

SECRET_KEY = 'fake-key'  # Needed for some internal operations