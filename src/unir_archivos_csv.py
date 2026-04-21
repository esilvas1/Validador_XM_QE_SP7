import pandas as pd
from pathlib import Path
import csv
import shutil
import tempfile

def combinar_csv(directorio_entrada, archivo_salida, patron='*.csv'):
    """
    Combina archivos CSV sin encabezados, preservando todos los datos exactamente.
    """
    try:
        dir_path = Path(directorio_entrada)

        if not dir_path.exists():
            raise FileNotFoundError(f"El directorio {directorio_entrada} no existe")

        archivos_csv = list(dir_path.glob(patron))

        if not archivos_csv:
            raise FileNotFoundError(f"No se encontraron archivos {patron} en {directorio_entrada}")

        #print(f"Se encontraron {len(archivos_csv)} archivos CSV para combinar")

        dfs = []
        numero_columnas = None

        for archivo in archivos_csv:
            print(f"\nProcesando: {archivo.name}")
            try:
                df = pd.read_csv(
                    archivo,
                    header=None,
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                    encoding='utf-8'
                )

                if df.empty:
                    print(f"ADVERTENCIA: {archivo.name} está vacío. Se omitirá.")
                    continue

                if df.iloc[:, 0].str.strip().eq('').all():
                    print(f"ADVERTENCIA: {archivo.name} tiene la primera columna vacía. Se omitirá.")
                    continue

                if numero_columnas is None:
                    numero_columnas = df.shape[1]
                    #print(f"Número de columnas establecido: {numero_columnas}")
                else:
                    if df.shape[1] != numero_columnas:
                        print(f"ADVERTENCIA: {archivo.name} tiene {df.shape[1]} columnas (se esperaban {numero_columnas}). Ajustando...")
                        if df.shape[1] > numero_columnas:
                            df = df.iloc[:, :numero_columnas]
                        else:
                            for i in range(df.shape[1], numero_columnas):
                                df[i] = ''

                dfs.append(df)
               #print(f"Archivo válido añadido. Registros: {len(df)}")

            except Exception as e:
                print(f"ERROR al procesar {archivo.name}: {str(e)}")
                continue

        if not dfs:
            raise ValueError("No se encontraron archivos CSV válidos para combinar")
        
        print(f"\nCombinando {len(dfs)} archivos CSV válidos...")
        df_combinado = pd.concat(dfs, axis=0, ignore_index=True)

        # ✅ Asegurarse de que la primera columna permanezca como texto
        print("Asegurando que la primera columna se mantenga como texto...")
        df_combinado[0] = df_combinado[0].astype(str) 


        # Guardar el archivo (escritura primero en temp local, luego mover al destino)
        print(f"Guardando el archivo combinado en: {archivo_salida}")
        archivo_salida_path = Path(archivo_salida)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv',
                                         encoding='utf-8', newline='') as tmp:
            tmp_path = tmp.name

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

        print("\n" + "=" * 50)
        #print(f"¡Proceso completado con éxito!")
        print(f"Archivos procesados válidos: {len(dfs)} de {len(archivos_csv)}")
        print(f"Archivo de salida: {archivo_salida}")
        print(f"Total registros combinados: {len(df_combinado)}")
        print(f"Columnas finales: {df_combinado.shape[1]}")

    except Exception as e:
        print(f"\nERROR CRÍTICO: {str(e)}")
