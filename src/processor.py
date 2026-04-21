import subprocess
import sys
import importlib
import os
import shutil
import tempfile
# from pandasgui import show  # No necesario en entorno web
import logging
import warnings
import pandas
from pandas import to_datetime
# import sweetviz as sv  # No necesario en entorno web

def _guardar_csv_via_temp(df, ruta_destino):
    """Escribe el DataFrame en un archivo temporal local y luego lo mueve al destino.
    Esto evita la escritura lenta fila-por-fila directamente sobre rutas de red/DFS."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv',
                                     encoding='utf-8', newline='') as tmp:
        tmp_path = tmp.name
    try:
        df.to_csv(tmp_path, index=False)
        shutil.move(tmp_path, ruta_destino)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def instalar_requisitos():
    ruta_requirements = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "requirements.txt"))
    if not os.path.exists(ruta_requirements):
        print(f"❌ El archivo {ruta_requirements} no existe.")
        sys.exit(1)

    with open(ruta_requirements, "r") as file:
        lineas = file.readlines()

    for linea in lineas:
        paquete = linea.strip()
        if not paquete or paquete.startswith("#"):
            continue  # Ignorar líneas vacías o comentarios

        # Extraer nombre del paquete sin versión (ej. "pandas" de "pandas==2.2.1")
        nombre_paquete = paquete.split("==")[0] if "==" in paquete else paquete

        try:
            importlib.import_module(nombre_paquete)
            print(f"✔️ '{nombre_paquete}' ya está instalado.")
        except ImportError:
            print(f"📦 Instalando '{paquete}'...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", paquete])
    print(f"ℹ️ Librerias instaladas correctamente")

def crear_CONSOLIDADO_QE():
    print("")
    print("crear archivo CONSOLIDADO_QE uniendo arhcivos CSV")
    from loader import get_data_dir, get_resultados_dir
    ruta_data = get_data_dir()
    ruta_resultados = get_resultados_dir()
    os.makedirs(ruta_resultados, exist_ok=True)
    directorio = os.path.join(ruta_data, 'Descargados QE')
    ruta_mas_nombre = os.path.join(ruta_data, 'CONSOLIDADO_QE_TEMP.csv')

    print("Extraccion de arcivos de QEnergia")
    """from unir_archivos_csv import combinar_csv
    combinar_csv(directorio, ruta_mas_nombre)"""

    from unir_archivos_zip import combinar_zip_con_csv
    combinar_zip_con_csv(directorio, ruta_mas_nombre)


    print("Cargar archivos CSV")
    print("Creando dataframe")
    from loader import cargar_csv
    CONSOLIDADO_QE  = cargar_csv("CONSOLIDADO_QE_TEMP",False) #crea el dataframe
    CONSOLIDADO_QE.columns = ["CODIGO_EVENTO", "FECHA_INI", "FECHA_FIN", "CODIGO_ELEMENTO", "TIPO_ELEMENTO", "CAUSA_CREG", "ESTADO_EVENTO", "ZNI", "AG" , "AP"] #asigna encabezado


    print("Eliminar filas donde CODIGO_EVENTO es NaN o solo espacios")
    print("Elminacion de filas con CODIGO_EVENTO = NaN")
    CONSOLIDADO_QE = CONSOLIDADO_QE[
        CONSOLIDADO_QE["CODIGO_EVENTO"].notna() &
        (CONSOLIDADO_QE["CODIGO_EVENTO"].astype(str).str.strip() != '')
        ]

    print("Agregando conlumna llave KEY")
    CONSOLIDADO_QE.insert(0, "KEY", CONSOLIDADO_QE["CODIGO_EVENTO"].astype(str) + "-" + CONSOLIDADO_QE["CODIGO_ELEMENTO"].astype(str)) #agrega una columna KEY

    print("crear df con solo cierres")
    print("Creando dataframe SOLO_CIERRES")
    SOLO_CIERRES = CONSOLIDADO_QE[
        CONSOLIDADO_QE["FECHA_INI"].isna()
        ]

    print("Elimina solo cierres de df: CONSOLIDADO_QE")
    print("Eliminacion de registro con solo cierres")
    CONSOLIDADO_QE = CONSOLIDADO_QE[
        CONSOLIDADO_QE["FECHA_INI"].notna()#no esta vacio - contiene un valor
        ]

    print("Traer los cierres al dataframe CONSOLIDADO_QE desde SOLO_CIERRES")
    print("Fusionar sin perder información")
    print("Creando columnas adcionales para traer informacion del cierre del evento")
    TEMP_MERGE = CONSOLIDADO_QE.merge(
        SOLO_CIERRES[["KEY", "FECHA_FIN"]],
        on="KEY",
        how="left",
        suffixes=('', '_cierre')
    )

    print("Solo actualizar FECHA_FIN si el valor original es vacío o NaN")
    print("Realizando actualización LEGO")
    TEMP_MERGE["FECHA_FIN"] = TEMP_MERGE["FECHA_FIN"].mask(
        TEMP_MERGE["FECHA_FIN"].astype(str).str.strip().isin(['', 'nan']),
        TEMP_MERGE["FECHA_FIN_cierre"]
    )

    print("Elimina las columnas extras del merge")
    print("Elminacion de columnas adcionales")
    CONSOLIDADO_QE = TEMP_MERGE.drop(columns=["FECHA_FIN_cierre"])

    print("crea el archivo csv CONSOLIDADO_QE")
    print("Creacion del archivo CONSOLIDADO_QE")
    if os.path.exists(os.path.join(ruta_resultados, 'CONSOLIDADO_QE.csv')):
        os.remove(os.path.join(ruta_resultados, 'CONSOLIDADO_QE.csv'))
        print(f"✅ Archivo eliminado: {os.path.join(ruta_resultados, 'CONSOLIDADO_QE.csv')}")
    else:
        print(f"⚠️ El archivo no existe: {os.path.join(ruta_resultados, 'CONSOLIDADO_QE.csv')}")

    _guardar_csv_via_temp(CONSOLIDADO_QE, os.path.join(ruta_resultados, 'CONSOLIDADO_QE.csv'))

    print("Eliminacion del archivo CONSOLIDADO_QE_TEMP")
    if os.path.exists(os.path.join(ruta_data, 'CONSOLIDADO_QE_TEMP.csv')):
        os.remove(os.path.join(ruta_data, 'CONSOLIDADO_QE_TEMP.csv'))
        print(f"✅ Archivo eliminado: {os.path.join(ruta_data, 'CONSOLIDADO_QE_TEMP.csv')}")
    else:
        print(f"⚠️ El archivo no existe: {os.path.join(ruta_data, 'CONSOLIDADO_QE_TEMP.csv')}")

    cantidad = CONSOLIDADO_QE[CONSOLIDADO_QE["FECHA_FIN"].isna()].shape[0]
    print(f"La cantidad de registros con FECHA_FIN vacía es: {cantidad}")

    # print("Silenciar INFO de pandasgui")  # No necesario en entorno web
    # logging.getLogger('pandasgui').setLevel(logging.CRITICAL)  # No necesario en entorno web

    print("Silenciar FutureWarnings")
    warnings.simplefilter(action='ignore', category=FutureWarning)

    print(f"✅ Data CONSOLIDADO_QE creada!")

    #show(CONSOLIDADO_QE)

def crear_CONSOLIDADO_SP7(forzar_descarga=False):

    print("");#salto de linea

    from loader import get_data_dir, get_resultados_dir, cargar_csv
    ruta_salida = get_data_dir()
    ruta_resultados = get_resultados_dir()
    os.makedirs(ruta_resultados, exist_ok=True)
    #directorio = os.path.join(ruta_salida, 'Descargados QE')

    print("crear el dataframe REPORTE_SP7")
    print("Paso 1: Descargando datos desde API SP7 (carga_sp7)...")
    from carga_sp7 import create_dataframe
    REPORTE_SP7 = create_dataframe(forzar_descarga=forzar_descarga)

    if REPORTE_SP7 is None or REPORTE_SP7.empty:
        raise Exception("No se pudo descargar datos de la API SP7. Verifique la conexión y las variables en .env")

    print("Descarga SP7 finalizada, continuando con el procesamiento...")

    print("Eliminar duplicados del dataframe")
    REPORTE_SP7 = REPORTE_SP7.drop_duplicates()
    
    print("Crear dataframe REPORTE_IUA")
    REPORTE_IUA = cargar_csv("cens_TransformadoresReportados", True)

    def _find_col(df, candidates):
        cols = list(df.columns)
        lower_map = {c.lower(): c for c in cols}
        for cand in candidates:
            if cand in cols:
                return cand
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        return None

    # Resolver columnas para soportar esquema API y CSV local
    col_nodo_sp7 = _find_col(REPORTE_SP7, ["Nodo", "NODO"])
    col_maniobra_sp7 = _find_col(REPORTE_SP7, ["Maniobra Apertura", "MANIOBRA_OPEN"])
    col_nodo_iua = _find_col(REPORTE_IUA, ["CodigoNodo", "CODIGONODO", "NODO"])
    col_iua = _find_col(REPORTE_IUA, ["IUA"])

    if not col_nodo_sp7:
        raise KeyError(f"No se encontró columna de nodo en REPORTE_SP7. Columnas: {list(REPORTE_SP7.columns)}")
    if not col_maniobra_sp7:
        raise KeyError(f"No se encontró columna de maniobra apertura en REPORTE_SP7. Columnas: {list(REPORTE_SP7.columns)}")
    if not col_nodo_iua or not col_iua:
        raise KeyError(f"No se encontraron columnas esperadas en REPORTE_IUA. Columnas: {list(REPORTE_IUA.columns)}")

    print("BUSCARV o LEFT OUTER JOIN para traer IUA desde REPORTE_IUA a REPORTE_SP7")
    REPORTE_SP7[col_nodo_sp7] = REPORTE_SP7[col_nodo_sp7].astype(str).str.strip()
    REPORTE_IUA[col_nodo_iua] = REPORTE_IUA[col_nodo_iua].astype(str).str.strip()

    REPORTE_SP7 = REPORTE_SP7.merge(
        REPORTE_IUA[[col_nodo_iua, col_iua]].rename(columns={col_nodo_iua: "CodigoNodo", col_iua: "IUA"}),
        left_on=col_nodo_sp7,
        right_on="CodigoNodo",
        how="left"
    )
    print("Elimina las columnas extras del merge")
    REPORTE_SP7 = REPORTE_SP7.drop(columns=["CodigoNodo"])
    
    print("Agregar columna KEY al df CONSOLIDADO_SP7")
    REPORTE_SP7.insert(0, "KEY",
                           REPORTE_SP7[col_maniobra_sp7].astype(str) +
                           "-" + 
                           REPORTE_SP7["IUA"].astype(str)
                           )  # agrega una columna KEY

    print("Crear CONSOLIDADO_SP7 en csv")
    _guardar_csv_via_temp(REPORTE_SP7, os.path.join(ruta_resultados, 'CONSOLIDADO_SP7.csv'))

    # print("Silenciar INFO de pandasgui")  # No necesario en entorno web
    # logging.getLogger('pandasgui').setLevel(logging.CRITICAL)  # No necesario en entorno web
    print("Silenciar FutureWarnings")
    warnings.simplefilter(action='ignore', category=FutureWarning)

    print("Print(REPORTE_SP7.columns)")
    print(f"✅ Data CONSOLIDADO_SP7 creada!")




    #report = sv.analyze(REPORTE_SP7)
    #report.show_html("reporte_df.html")  # Se abrirá automáticamente en el navegador

    #show(REPORTE_SP7)


def crear_CONSOLIDADO_XM():

    # crear archivo CONSOLIDADO_XM uniendo arhcivos CSV
    print("")#salto de linea
    from loader import get_data_dir, get_resultados_dir
    ruta_salida = get_data_dir()
    ruta_resultados = get_resultados_dir()
    os.makedirs(ruta_resultados, exist_ok=True)
    directorio = os.path.join(ruta_salida, 'Descargados XM')
    ruta_mas_nombre = os.path.join(ruta_salida, 'CONSOLIDADO_XM_TEMP.csv')

    print("Extraccion de arcivos de XM")
    from unir_archivos_csv import combinar_csv
    combinar_csv(directorio, ruta_mas_nombre)

    # --- Cargar archivos CSV ---
    print("Creando dataframe")
    from loader import cargar_csv
    CONSOLIDADO_XM  = cargar_csv("CONSOLIDADO_XM_TEMP",False) #crea el dataframe
    CONSOLIDADO_XM.columns = ["CODIGO_EVENTO",
                              "FECHA_INI",
                              "FECHA_FIN",
                              "CODIGO_ELEMENTO",
                              "TIPO_ELEMENTO",
                              "CAUSA_CREG",
                              "ESTADO_EVENTO",
                              "ZNI",
                              "AG" ,
                              "AP"] #asigna encabezado


    # Eliminar filas donde CODIGO_EVENTO es NaN o solo espacios
    print("Elminacion de filas con CODIGO_EVENTO = NaN")
    CONSOLIDADO_XM = CONSOLIDADO_XM[
        CONSOLIDADO_XM["CODIGO_EVENTO"].notna() &
        (CONSOLIDADO_XM["CODIGO_EVENTO"].astype(str).str.strip() != '')
        ]

    print("Agregando conlumna llave KEY")
    CONSOLIDADO_XM.insert(0, "KEY", CONSOLIDADO_XM["CODIGO_EVENTO"].astype(str) + "-" + CONSOLIDADO_XM["CODIGO_ELEMENTO"].astype(str)) #agrega una columna KEY

    #crear df con solo cierres
    print("Creando dataframe SOLO_CIERRES")
    SOLO_CIERRES = CONSOLIDADO_XM[
        CONSOLIDADO_XM["FECHA_INI"].isna()
        ]

    #elimina solo cierres de df: CONSOLIDADO_QE
    print("Eliminacion de registro con solo cierres")
    CONSOLIDADO_XM = CONSOLIDADO_XM[
        CONSOLIDADO_XM["FECHA_INI"].notna()#no esta vacio - contiene un valor
        ]

    #Traer los cierres al dataframe CONSOLIDADO_QE desde SOLO_CIERRES
    # Fusionar sin perder información
    print("Creando columnas adcionales para traer informacion del cierre del evento")
    TEMP_MERGE = CONSOLIDADO_XM.merge(
        SOLO_CIERRES[["KEY", "FECHA_FIN"]],
        on="KEY",
        how="left",
        suffixes=('', '_cierre')
    )

    # Solo actualizar FECHA_FIN si el valor original es vacío o NaN
    print("Realizando actualización LEGO")
    TEMP_MERGE["FECHA_FIN"] = TEMP_MERGE["FECHA_FIN"].mask(
        TEMP_MERGE["FECHA_FIN"].astype(str).str.strip().isin(['', 'nan']),
        TEMP_MERGE["FECHA_FIN_cierre"]
    )

    # Elimina las columnas extras del merge
    print("Elminacion de columnas adcionales")
    CONSOLIDADO_XM = TEMP_MERGE.drop(columns=["FECHA_FIN_cierre"])

    #crea el archivo csv CONSOLIDADO_QE
    print("Creacion del archivo CONSOLIDADO_XM")
    if os.path.exists(os.path.join(ruta_resultados, 'CONSOLIDADO_XM.csv')):
        os.remove(os.path.join(ruta_resultados, 'CONSOLIDADO_XM.csv'))
        print(f"✅ Archivo eliminado: {os.path.join(ruta_resultados, 'CONSOLIDADO_XM.csv')}")
    else:
        print(f"⚠️ El archivo no existe: {os.path.join(ruta_resultados, 'CONSOLIDADO_XM.csv')}")

    print(f"Guardando el archivo combinado en: {os.path.join(ruta_resultados, 'CONSOLIDADO_XM.csv')}")
    _guardar_csv_via_temp(CONSOLIDADO_XM, os.path.join(ruta_resultados, 'CONSOLIDADO_XM.csv'))

    #Elimina el archivo csv CONSOLIDADO_QE_TEMP
    print("Eliminacion del archivo CONSOLIDADO_XM_TEMP")
    if os.path.exists(os.path.join(ruta_salida, 'CONSOLIDADO_XM_TEMP.csv')):
        os.remove(os.path.join(ruta_salida, 'CONSOLIDADO_XM_TEMP.csv'))
        print(f"✅ Archivo eliminado: {os.path.join(ruta_salida, 'CONSOLIDADO_XM_TEMP.csv')}")
    else:
        print(f"⚠️ El archivo no existe: {os.path.join(ruta_salida, 'CONSOLIDADO_XM_TEMP.csv')}")

    cantidad = CONSOLIDADO_XM[CONSOLIDADO_XM["FECHA_FIN"].isna()].shape[0]

  
    #print("Silenciar FutureWarnings")
    #warnings.simplefilter(action='ignore', category=FutureWarning)

    print(f"Count of FECHA_FIN IS NULL: {cantidad}")

    print(f"✅ Data CONSOLIDADO_XM creada!")
