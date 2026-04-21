import oracledb
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# oracledb usa modo thin por defecto (no requiere Oracle Client)
# Si necesitas modo thick, descomenta: oracledb.init_oracle_client()

def open_conexion():
    oracle_host = os.environ.get('ORACLE_HOST')
    oracle_port = os.environ.get('ORACLE_PORT')
    oracle_service_name = os.environ.get('ORACLE_SERVICE_NAME')
    oracle_user = os.environ.get('ORACLE_USER')
    oracle_password = os.environ.get('ORACLE_PASSWORD')

    if not all([oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password]):
        print("❌ Error: Faltan variables de conexión en el archivo .env")
        return None, None

    # Usar formato de conexión Easy Connect Plus para oracledb
    dsn = f"{oracle_host}:{oracle_port}/{oracle_service_name}"

    # Crear engine con oracledb explícito y thick_mode=False para modo thin
    engine = create_engine(
        f"oracle+oracledb://{oracle_user}:{oracle_password}@{dsn}",
        poolclass=NullPool  # Evita problemas de pool con conexiones manuales
    )

    try:
        conn = engine.connect()
        print("✅ BRAE DATABASE - ¡Conectado exitosamente!")
        return conn, engine
    except Exception as e:
        print(f"❌ Error de conexión:\n{e}")
        return None, None