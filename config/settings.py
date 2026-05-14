"""
Django settings for Validacion Ajuste Mensual project.
"""

from pathlib import Path
import os
import logging
from dotenv import load_dotenv
from django.core.exceptions import ImproperlyConfigured

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)

# Cargar variables desde .env
load_dotenv(BASE_DIR / '.env')

X_FRAME_OPTIONS = "ALLOWALL"
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-change-me')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', 'True').lower() in ('true', '1', 'yes')

ALLOWED_HOSTS = os.environ.get('ALLOWED_HOSTS', '*').split(',')

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'validacion_app',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'validacion_app.middleware.HerramientasLoginRequiredMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
LANGUAGE_CODE = 'es-es'
TIME_ZONE = 'America/Bogota'
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATICFILES_DIRS = [BASE_DIR / 'static']

# Media files
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Custom settings for data paths
data_dir_env = os.environ.get('DATA_DIR', '').strip()

# En Linux permitir override con ruta POSIX montada para despliegue en contenedor.
if os.name != 'nt':
    data_dir_linux = os.environ.get('DATA_DIR_LINUX', '').strip()
    if data_dir_linux:
        data_dir_env = data_dir_linux

if not data_dir_env:
    raise ImproperlyConfigured(
        "La variable DATA_DIR es obligatoria y debe apuntar al DFS "
        "(en Linux puede usar DATA_DIR_LINUX como override)."
    )

DATA_DIR = Path(data_dir_env)

# Tabla Oracle para usuarios de la pestaña Herramientas (columnas USERNAME / PASSWORD por defecto).
# Nombre de tabla (solo nombre o ESQUEMA.TABLA). Columnas reales típicas: USERNAME, PASSWORD, ROL, ID.
ORACLE_USUARIOS_TABLA = os.environ.get('ORACLE_USUARIOS_TABLA', 'S0022_USUARIOS').strip() or 'S0022_USUARIOS'
ORACLE_USUARIOS_COL_USUARIO = os.environ.get('ORACLE_USUARIOS_COL_USUARIO', 'USERNAME').strip() or 'USERNAME'
ORACLE_USUARIOS_COL_PASSWORD = os.environ.get('ORACLE_USUARIOS_COL_PASSWORD', 'PASSWORD').strip() or 'PASSWORD'
# Esquema/propietario explícito de la tabla (opcional). Si está vacío, se puede usar ORACLE_CIM_OWNER.
ORACLE_USUARIOS_ESQUEMA = os.environ.get('ORACLE_USUARIOS_ESQUEMA', '').strip()

# Oracle CIM: instancia donde está S0022_USUARIOS. Si las cinco variables están definidas,
# el login Herramientas usa solo esta conexión para leer usuarios (no ORACLE_HOST).
ORACLE_CIM_HOST = os.environ.get('ORACLE_CIM_HOST', '').strip()
ORACLE_CIM_PORT = os.environ.get('ORACLE_CIM_PORT', '').strip()
# Acepta ORACLE_CIM_SERVICE_NAME o el alias corto ORACLE_CIM_SERVICE
ORACLE_CIM_SERVICE_NAME = (
    os.environ.get('ORACLE_CIM_SERVICE_NAME')
    or os.environ.get('ORACLE_CIM_SERVICE')
    or ''
).strip()
ORACLE_CIM_USER = os.environ.get('ORACLE_CIM_USER', '').strip()
ORACLE_CIM_PASSWORD = os.environ.get('ORACLE_CIM_PASSWORD', '').strip()
ORACLE_CIM_CONFIGURED = bool(
    ORACLE_CIM_HOST
    and ORACLE_CIM_PORT
    and ORACLE_CIM_SERVICE_NAME
    and ORACLE_CIM_USER
    and ORACLE_CIM_PASSWORD
)
# Propietario/esquema de la tabla en CIM (ej. CIM.S0022_USUARIOS). Se usa si ORACLE_USUARIOS_ESQUEMA está vacío.
ORACLE_CIM_OWNER = os.environ.get('ORACLE_CIM_OWNER', '').strip()

# Mantener modo DFS-only, pero no impedir el arranque del proceso web.
# Los procesos que usen DFS validan accesibilidad en tiempo de ejecución.
if os.name != 'nt' and data_dir_env.startswith('\\\\'):
    logger.warning(
        "DATA_DIR parece una ruta UNC de Windows en Linux: %s. "
        "Monte el DFS y use una ruta POSIX en DATA_DIR (ej: /mnt/dfs/... ).",
        data_dir_env,
    )

if not DATA_DIR.exists() or not DATA_DIR.is_dir():
    logger.warning(
        "DATA_DIR no existe o no es accesible en arranque: %s. "
        "La app iniciara, pero los procesos de negocio fallaran hasta que el DFS sea accesible.",
        DATA_DIR,
    )

OUTPUT_DIR = BASE_DIR / 'output'
