import pandas as pd
from pathlib import Path
import csv
import zipfile
import io
import shutil
import tempfile
import os

def combinar_zip_con_csv(directorio_entrada, archivo_salida, patron='*.zip'):
    """
    Extrae y combina archivos CSV desde archivos ZIP, sin encabezados,
    preservando todos los datos exactamente.
    """
    try:
        dir_path = Path(directorio_entrada)

        if not dir_path.exists():
            raise FileNotFoundError(f"El directorio {directorio_entrada} no existe")

        archivos_zip = list(dir_path.glob(patron))

        if not archivos_zip:
            raise FileNotFoundError(f"No se encontraron archivos {patron} en {directorio_entrada}")

        dfs = []
        numero_columnas = None

        for archivo_zip in archivos_zip:
            try:
                with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
                    nombres_archivos = zip_ref.namelist()

                    for nombre in nombres_archivos:
                        print(f"\nProcesando: {nombre} en {archivo_zip.name}")
                        if not nombre.lower().endswith('.csv'):
                            continue  # Ignora archivos que no sean CSV

                        with zip_ref.open(nombre) as file:
                            try:
                                print(f"Leyendo {nombre} desde {archivo_zip.name}...")
                                df = pd.read_csv(
                                    io.TextIOWrapper(file, encoding='utf-8'),
                                    header=None,
                                    dtype=str,
                                    keep_default_na=False,
                                    na_filter=False
                                )

                                if df.empty:
                                    print(f"ADVERTENCIA: {nombre} en {archivo_zip.name} está vacío. Se omitirá.")
                                    continue

                                if df.iloc[:, 0].str.strip().eq('').all():
                                    print(f"ADVERTENCIA: {nombre} en {archivo_zip.name} tiene la primera columna vacía. Se omitirá.")
                                    continue

                                if numero_columnas is None:
                                    numero_columnas = df.shape[1]
                                else:
                                    if df.shape[1] != numero_columnas:
                                        print(f"ADVERTENCIA: {nombre} en {archivo_zip.name} tiene {df.shape[1]} columnas (se esperaban {numero_columnas}). Ajustando...")
                                        if df.shape[1] > numero_columnas:
                                            df = df.iloc[:, :numero_columnas]
                                        else:
                                            for i in range(df.shape[1], numero_columnas):
                                                df[i] = ''

                                dfs.append(df)

                            except Exception as e:
                                print(f"ERROR al procesar {nombre} en {archivo_zip.name}: {str(e)}")
                                continue

            except zipfile.BadZipFile:
                print(f"ERROR: {archivo_zip.name} no es un archivo ZIP válido.")
                continue

        if not dfs:
            raise ValueError("No se encontraron archivos CSV válidos dentro de los ZIP para combinar")
        
        print(f"\nCombinando {len(dfs)} archivos CSV válidos extraídos de ZIP...")
        df_combinado = pd.concat(dfs, axis=0, ignore_index=True)
        df_combinado[0] = df_combinado[0].astype(str)

        # Escritura primero en temp local, luego mover al destino (evita escritura lenta sobre red/DFS)
        print(f"Guardando el archivo combinado en: {archivo_salida}")
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv',
                                         encoding='utf-8', newline='') as tmp:
            tmp_path = tmp.name
        try:
            df_combinado.to_csv(
                tmp_path,
                index=False,
                header=False,
                encoding='utf-8',
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                lineterminator='\n'
            )
            shutil.move(tmp_path, archivo_salida)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
        print(f"Archivo de salida: {archivo_salida}")
        print(f"Total registros combinados: {len(df_combinado)}")
        print(f"Columnas finales: {df_combinado.shape[1]}")

    except Exception as e:
        print(f"\nERROR CRÍTICO: {str(e)}")
