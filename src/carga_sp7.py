import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import glob
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from calendar import monthrange
from dotenv import load_dotenv
import time

# Cargar variables desde .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Configuración de reintentos y tiempos
MAX_REINTENTOS = 5          # Reintentos por cada día
TIMEOUT_SEGUNDOS = 120      # Timeout por petición (2 minutos)
PAUSA_ENTRE_DIAS = 5        # Segundos de espera entre cada día
PAUSA_ENTRE_REINTENTOS = 10 # Segundos base de espera entre reintentos


def crear_sesion():
    """Crea una sesión HTTP con reintentos automáticos a nivel de conexión."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def obtener_token(session):
    """Obtiene el token de autenticación de la API."""
    auth_url = os.environ.get('API_URL')
    usuario = os.environ.get('API_USER')
    password = os.environ.get('API_PASSWORD')

    if not all([auth_url, usuario, password]):
        print("❌ Error: Faltan variables de API (API_URL, API_USER, API_PASSWORD) en el archivo .env")
        return None

    auth_payload = {
        "userID": usuario,
        "password": password
    }

    headers_auth = {
        "Content-Type": "application/json"
    }

    response_auth = session.post(auth_url, json=auth_payload, headers=headers_auth, timeout=TIMEOUT_SEGUNDOS)
    response_auth.raise_for_status()

    token = response_auth.json().get("accessToken")
    if not token:
        raise ValueError("No se recibió un token válido.")

    return token


def consultar_dia(session, token, fecha_desde_str, fecha_hasta_str):
    """Consulta los datos de un solo día a la API con timeout."""
    consulta_base_url = os.environ.get('API_CONSULTA_URL')
    consulta_url = f"{consulta_base_url}?desde={fecha_desde_str}&hasta={fecha_hasta_str}"

    headers_consulta = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = session.get(consulta_url, headers=headers_consulta, timeout=TIMEOUT_SEGUNDOS)
    response.raise_for_status()

    data = response.json()

    if isinstance(data, list):
        return pd.DataFrame(data)
    else:
        return pd.DataFrame([data])


def descargar_dia_con_reintentos(session, token, dia, anio, mes, ultimo_dia):
    """Intenta descargar un día con múltiples reintentos y espera progresiva."""
    fecha_inicio = datetime(anio, mes, dia, 0, 0, 0)
    fecha_fin = fecha_inicio + timedelta(days=1)

    desde_str = fecha_inicio.strftime("%Y-%m-%d %H:%M:%S.000")
    hasta_str = fecha_fin.strftime("%Y-%m-%d %H:%M:%S.000")

    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            if intento == 1:
                print(f"  📥 Día {dia:02d}/{ultimo_dia} — {desde_str} → {hasta_str} ... ", end="", flush=True)
            else:
                print(f"     ↻ Reintento {intento}/{MAX_REINTENTOS} ... ", end="", flush=True)

            df_dia = consultar_dia(session, token, desde_str, hasta_str)
            registros = len(df_dia)
            print(f"OK ({registros} registros)")
            return df_dia

        except (requests.exceptions.RequestException, ConnectionError, OSError) as e:
            error_corto = str(e).split("(Caused by")[0].strip() if "(Caused by" in str(e) else str(e)[:80]
            print(f"ERROR ({error_corto})")

            if intento < MAX_REINTENTOS:
                espera = PAUSA_ENTRE_REINTENTOS * intento  # Espera progresiva
                print(f"     ⏳ Esperando {espera}s antes de reintentar...")
                time.sleep(espera)
            else:
                print(f"     ❌ Día {dia:02d} agotó todos los reintentos.")

    return None


def cargar_progreso(nombre_csv):
    """Carga el CSV parcial si existe, para retomar la descarga."""
    if os.path.exists(nombre_csv):
        try:
            df = pd.read_csv(nombre_csv)
            print(f"📂 Archivo parcial encontrado: {nombre_csv} ({len(df)} registros)")
            return df
        except Exception:
            pass
    return None


def _leer_csv_o_zip_en_ruta(base_dir, nombre_base_busqueda):
    """Busca y carga un CSV de SP7 (directo o dentro de ZIP) en una ruta dada.
    Ignora sufijos de versión/período y toma el archivo más reciente."""
    if not base_dir or not os.path.isdir(base_dir):
        return None

    patron_csv = os.path.join(base_dir, f"*{nombre_base_busqueda}*.csv")
    candidatos_csv = sorted(
        glob.glob(patron_csv),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )

    # Preferir archivos finales sobre archivos parciales
    candidatos_csv_finales = [p for p in candidatos_csv if "_parcial" not in os.path.basename(p).lower()]
    if candidatos_csv_finales:
        candidatos_csv = candidatos_csv_finales

    if candidatos_csv:
        ruta_csv = candidatos_csv[0]
        try:
            df_existente = pd.read_csv(ruta_csv, dtype=str)
            print(f"⚡ CSV encontrado en '{base_dir}', cargando sin llamar a la API: {ruta_csv}")
            print(f"   ✅ Cargado: {len(df_existente)} registros")
            return df_existente
        except Exception as e:
            print(f"   ⚠️  No se pudo leer '{ruta_csv}': {e}")

    patron_zip = os.path.join(base_dir, f"*{nombre_base_busqueda}*.zip")
    candidatos_zip = sorted(
        glob.glob(patron_zip),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )
    for zip_path in candidatos_zip:
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_dentro = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if csv_dentro:
                    with zf.open(csv_dentro[0]) as cf:
                        df_existente = pd.read_csv(cf, dtype=str)
                    print(f"⚡ CSV encontrado dentro de ZIP en '{base_dir}', cargando sin llamar a la API: {zip_path}")
                    print(f"   ✅ Cargado: {len(df_existente)} registros")
                    return df_existente
        except Exception as e:
            print(f"   ⚠️  No se pudo leer ZIP '{zip_path}': {e}")

    return None


def create_dataframe(forzar_descarga=False, solo_existente=False):
    """Descarga datos día a día para el mes completo con reintentos y guardado incremental."""
    fecha_desde_env = os.environ.get('API_FECHA_DESDE')

    if not fecha_desde_env:
        print("❌ Error: Falta la variable API_FECHA_DESDE en el archivo .env")
        return None

    # Determinar el mes y año a partir de API_FECHA_DESDE
    fecha_base = datetime.strptime(fecha_desde_env.strip(), "%Y-%m-%d %H:%M:%S.%f")
    anio = fecha_base.year
    mes = fecha_base.month
    _, ultimo_dia_mes = monthrange(anio, mes)

    # Si estamos en el mismo mes y año, descargar solo hasta el día actual
    hoy = datetime.now()
    if anio == hoy.year and mes == hoy.month:
        ultimo_dia = hoy.day
        print(f"📅 Mes en curso: descargando datos de {anio}-{mes:02d} (días 1 al {ultimo_dia} — hoy)")
    else:
        ultimo_dia = ultimo_dia_mes
        print(f"📅 Mes completo: descargando datos de {anio}-{mes:02d} (días 1 al {ultimo_dia})")

    nombre_csv = f"Consulta_Eventos_Cens_Apertura_Cierre_{anio}_{mes:02d}.csv"
    nombre_csv_parcial = f"Consulta_Eventos_Cens_Apertura_Cierre_{anio}_{mes:02d}_parcial.csv"

    # ---------------------------------------------------------------
    # REUTILIZAR DESCARGA PREVIA:
    # Solo se usa DATA_DIR/Reporte Mensual (DFS).
    # ---------------------------------------------------------------
    from loader import get_data_dir
    try:
        data_dir = get_data_dir()
    except Exception as e:
        print(f"❌ Error de DATA_DIR: {e}")
        return None

    carpeta_rm = os.path.join(os.path.abspath(data_dir), 'Reporte Mensual')
    try:
        os.makedirs(carpeta_rm, exist_ok=True)
    except Exception as e:
        print(f"❌ Error: No se pudo acceder/crear '{carpeta_rm}': {e}")
        return None

    ruta_csv = os.path.join(carpeta_rm, nombre_csv)
    ruta_csv_parcial = os.path.join(carpeta_rm, nombre_csv_parcial)

    # Solo se usa el prefijo base del archivo. Se ignora cualquier sufijo/version/período.
    nombre_base_busqueda = "Consulta_Eventos_Cens_Apertura_Cierre"

    if not forzar_descarga:
        print(f"🔍 Buscando descarga previa en: {carpeta_rm}")
        df_existente = _leer_csv_o_zip_en_ruta(carpeta_rm, nombre_base_busqueda)
        if df_existente is not None and not df_existente.empty:
            return df_existente

        if solo_existente:
            print("❌ Se solicitó usar archivo existente y no se encontró uno utilizable. No se llamará la API SP7.")
            return None

        print("   ℹ️  No se encontró una descarga previa utilizable. Iniciando descarga desde API SP7...")
    else:
        print("⚠️  Descarga forzada activada por el usuario. Se consultará la API SP7.")
    # ---------------------------------------------------------------

    try:
        # Crear sesión con reintentos
        session = crear_sesion()

        # Paso 1: Obtener token
        token = obtener_token(session)
        if not token:
            return None
        print("✅ Token obtenido correctamente.")
        print(f"⚙️  Config: timeout={TIMEOUT_SEGUNDOS}s, reintentos={MAX_REINTENTOS}, pausa={PAUSA_ENTRE_DIAS}s")
        print("Esto puede tardar varios minutos, por favor espere ...\n")

        # Paso 2: Descargar día a día
        dfs = []
        dias_exitosos = 0
        dias_fallidos = []

        for dia in range(1, ultimo_dia + 1):
            df_dia = descargar_dia_con_reintentos(session, token, dia, anio, mes, ultimo_dia)

            if df_dia is not None and len(df_dia) > 0:
                dfs.append(df_dia)
                dias_exitosos += 1

                # Guardado incremental: guardar progreso después de cada día exitoso
                df_parcial = pd.concat(dfs, ignore_index=True)
                df_parcial.to_csv(ruta_csv_parcial, index=False)
            else:
                dias_fallidos.append(dia)

            # Pausa entre días para no sobrecargar el servidor
            if dia < ultimo_dia:
                time.sleep(PAUSA_ENTRE_DIAS)

        # Paso 3: Guardar archivo final
        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)

            # Limpiar versiones anteriores dentro de DATA_DIR/Reporte Mensual
            archivos_existentes = glob.glob(os.path.join(carpeta_rm, f"*{nombre_base_busqueda}*"))
            for archivo_viejo in archivos_existentes:
                if os.path.abspath(archivo_viejo) in {
                    os.path.abspath(ruta_csv),
                    os.path.abspath(ruta_csv_parcial),
                }:
                    continue
                try:
                    os.remove(archivo_viejo)
                    print(f"   🗑️  Eliminado archivo anterior: {os.path.basename(archivo_viejo)}")
                except Exception as e:
                    print(f"   ⚠️  No se pudo eliminar '{archivo_viejo}': {e}")

            df_final.to_csv(ruta_csv, index=False)

            # Eliminar archivo parcial si la descarga terminó
            if os.path.exists(ruta_csv_parcial):
                os.remove(ruta_csv_parcial)

            print(f"   📁 Archivo guardado en DFS: {ruta_csv}")

            print(f"\n{'='*60}")
            print(f"✅ Descarga completada:")
            print(f"   - Días exitosos: {dias_exitosos}/{ultimo_dia}")
            if dias_fallidos:
                print(f"   - Días fallidos: {dias_fallidos}")
            print(f"   - Total registros: {len(df_final)}")
            print(f"   - Archivo guardado: {ruta_csv}")
            print(f"{'='*60}")

            return df_final
        else:
            print("\n❌ No se obtuvieron datos de ningún día.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud de autenticación: {e}")
    except ValueError as e:
        print(f"Error en autenticación: {e}")
    except KeyboardInterrupt:
        print("\n\n⚠️  Descarga interrumpida por el usuario.")
        if os.path.exists(ruta_csv_parcial):
            print(f"   Los datos descargados hasta ahora se guardaron en: {ruta_csv_parcial}")
    except Exception as e:
        print(f"Error inesperado: {e}")
        if os.path.exists(ruta_csv_parcial):
            print(f"   Los datos parciales se guardaron en: {ruta_csv_parcial}")


if __name__ == "__main__":
    create_dataframe()
