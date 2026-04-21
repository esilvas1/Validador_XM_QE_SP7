import subprocess
import sys
import importlib
import os
# from pandasgui import show  # No necesario en entorno web
import logging
import warnings
import pandas
from pandas import to_datetime
# import sweetviz as sv  # No necesario en entorno web
import numpy

def crear_QA_TFDDREGISTRO():

    print("");#salto de linea

    from loader import get_data_dir
    ruta_salida = get_data_dir()

    #Create the dataframe QA_TFDDREGISTRO
    print("create the dataframe QA_TFDDREGISTRO\n")
    from loader import cargar_csv
    QA_TFDDREGISTRO = cargar_csv("CONSOLIDADO_SP7", True)
    QA_TFDDCAUSAS_SP7 = cargar_csv("QA_TFDDCAUSAS_SP7", True)
    QA_TFDDCAUSAS_OMS = cargar_csv("QA_TFDDCAUSAS_OMS", True)
    QA_TTT2_REGISTRO = cargar_csv("QA_TTT2_REGISTRO", True)
    print("The CONSOLIDADO_SP7 dataframe information has been uploaded successfully\n")

    #Columns of the QA_TFDDREGISTRO dataframe
    print("Columns of the QA_TFDDREGISTRO dataframe: \n")
    print('KEY', 'IDE_Evento', 'IDE_Circuito', 'Elemento_Falla', 'Nodo',
           'Tipo Elemento', 'Maniobra Apertura', 'Maniobra Cierre',
           'Fecha_Desenergizacion', 'Fecha_Energizacion', 'Clientes ',
           'Descripcion_Codigo', 'Codigo', 'Area_Responsabilidad', 'Estado Evento',
           'Usuario_Creador', 'Usuario_Modificacion', 'Trafos Afectados',
           'Fecha creación Evento', 'Fecha modificación Evento', 'Borrado', 'IUA',
           'DURACION_min', 'REPORTE_MM', 'REPORTE_DD', 'CausasNoReportado',
           'ESTADO_VALIDACION\n')

    #Create the condition of filter for take all the records belong to the data
    print("Create the condition of filter for take all the records belong to the data\n")
    condicion = QA_TFDDREGISTRO['ESTADO_VALIDACION'].isin(['Reporte DD', 'Agregado MM', 'Modificado MM', 'Eliminado MM'])
    print("Condition has been created: "
          "condicion = QA_TFDDREGISTRO['ESTADO_VALIDACION'].isin(['Reporte DD', 'Agregado MM', 'Modificado MM', 'Eliminado MM')]'\n")

    #Create a new dataframe with the before condition
    print("Create a new dataframe with the before condition\n")
    QA_TFDDREGISTRO = QA_TFDDREGISTRO[condicion].copy()
    print("New filter created for QA_TFDDREGISTRO\n")

    #Format the date fields to use in operations into the code
    print("Format the date fields in datetime64 to use in operations into the code\n")
    formato = "%d/%m/%Y %H:%M:%S.%f"
    QA_TFDDREGISTRO["Fecha_Desenergizacion"] = pandas.to_datetime(QA_TFDDREGISTRO["Fecha_Desenergizacion"], errors='coerce')#, dayfirst=True)
    QA_TFDDREGISTRO["Fecha_Energizacion"] = pandas.to_datetime(QA_TFDDREGISTRO["Fecha_Energizacion"], errors='coerce') #, dayfirst=True)

    #Craete QA_TFDDREGISTRO table structure as in the database BRAE exits
    print("Create QA_TFDDREGISTRO table structure as in the database BRAE exists\n")

    print("Add columns belongs to QA_TFDDREGISTRO from BRAE\n")
    QA_TFDDREGISTRO["FDD_CODIGOEVENTO"] = QA_TFDDREGISTRO["Maniobra Apertura"]
    QA_TFDDREGISTRO["FDD_FINICIAL"] = QA_TFDDREGISTRO["Fecha_Desenergizacion"]
    QA_TFDDREGISTRO["FDD_FFINAL"] = QA_TFDDREGISTRO["Fecha_Energizacion"]
    QA_TFDDREGISTRO["FDD_CODIGOELEMENTO"] = QA_TFDDREGISTRO["Nodo"]
    QA_TFDDREGISTRO["FDD_TIPOELEMENTO"] = "Transformer"
    QA_TFDDREGISTRO["FDD_CONSUMODIA"] = "0"
    QA_TFDDREGISTRO["FDD_ENS_ELEMENTO"] = "0"
    QA_TFDDREGISTRO["FDD_ENS_EVENTO"] = "0"
    QA_TFDDREGISTRO["FDD_ENEG_EVENTO"] = "0"
    QA_TFDDREGISTRO["FDD_ENEG_ELEMENTO"] = "0"
    QA_TFDDREGISTRO["FDD_CODIGOGENERADOR"] = "0"
    QA_TFDDREGISTRO["FDD_CAUSA"] = QA_TFDDREGISTRO["Codigo"]
    QA_TFDDREGISTRO["FDD_CAUSA_CREG"] = "assingned by process"
    QA_TFDDREGISTRO["FDD_USUARIOAP"] = "assingned by process"
    QA_TFDDREGISTRO["FDD_CONTINUIDAD"] = numpy.where(QA_TFDDREGISTRO["Fecha_Energizacion"].isnull(), 'S', 'N')
    QA_TFDDREGISTRO["FDD_ESTADOREPORTE"] = "S"
    QA_TFDDREGISTRO["FDD_PUBLICADO"] = "S"
    QA_TFDDREGISTRO["FDD_RECONFIG"] = numpy.where(QA_TFDDREGISTRO["Fecha_Desenergizacion"].dt.date == QA_TFDDREGISTRO["Fecha_Energizacion"].dt.date, 'N', 'S')
    QA_TFDDREGISTRO["FDD_PERIODO_OP"] = numpy.where((QA_TFDDREGISTRO["Fecha_Desenergizacion"].dt.normalize() == QA_TFDDREGISTRO["Fecha_Energizacion"].dt.normalize()) | (QA_TFDDREGISTRO["Fecha_Energizacion"].isnull()), QA_TFDDREGISTRO["Fecha_Desenergizacion"].dt.normalize(), QA_TFDDREGISTRO["Fecha_Energizacion"].dt.normalize())
    QA_TFDDREGISTRO["FDD_FREG_APERTURA"] = ""
    QA_TFDDREGISTRO["FDD_FREG_CIERRE"] = ""
    QA_TFDDREGISTRO["FDD_FPUB_APERTURA"] = ""
    QA_TFDDREGISTRO["FDD_FPUB_CIERRE"] = ""
    QA_TFDDREGISTRO["FDD_PERIODO_TC1"] = "assingned by process" #traer de la data CONSOLIDADO_SP7
    QA_TFDDREGISTRO["FDD_TIPOCARGA"] = "DR"
    QA_TFDDREGISTRO["FDD_EXCLUSION"] = "assingned by process"
    QA_TFDDREGISTRO["FDD_CAUSA_SSPD"] = "assigned by process"
    QA_TFDDREGISTRO["FDD_AJUSTADO"] = numpy.where(QA_TFDDREGISTRO["REPORTE_MM"] == "0", "N", "S")
    QA_TFDDREGISTRO["FDD_TIPOAJUSTE"] = QA_TFDDREGISTRO["REPORTE_MM"]
    QA_TFDDREGISTRO["FDD_RADICADO"] = "NA"
    print("  → FDD_PERIODO_TC1, FDD_TIPOCARGA, FDD_EXCLUSION, FDD_CAUSA_SSPD, FDD_AJUSTADO, FDD_TIPOAJUSTE, FDD_RADICADO OK")

    # --- Columnas de elemento de falla e IUA ---
    print("  → Asignando FDD_ELEMENTOFALLA e FDD_IUA...")
    # Detectar nombre real de la columna Elemento_Falla (puede tener \n al final)
    col_elemento_falla = next(
        (c for c in QA_TFDDREGISTRO.columns if c.strip() == 'Elemento_Falla'),
        None
    )
    if col_elemento_falla is None:
        raise KeyError("No se encontró la columna 'Elemento_Falla' en el DataFrame")
    QA_TFDDREGISTRO["FDD_ELEMENTOFALLA"] = QA_TFDDREGISTRO[col_elemento_falla]
    QA_TFDDREGISTRO["FDD_IUA"] = QA_TFDDREGISTRO["IUA"]
    print("  → FDD_ELEMENTOFALLA (columna origen: '{}'), FDD_IUA OK".format(col_elemento_falla.strip()))

    #Delete columns that isn't belong to QA_TFDDREGISTRO from BRAE databases
    print("Delete columns that's not belongs to QA_TFDDREGISTRO from BRAE database\n")
    base_targets = ['KEY', 'IDE_Evento', 'IDE_Circuito', 'Elemento_Falla', 'Nodo',
           'Tipo Elemento', 'Maniobra Apertura', 'Maniobra Cierre',
           'Fecha_Desenergizacion', 'Fecha_Energizacion', 'Clientes',
           'Descripcion_Codigo', 'Codigo', 'Area_Responsabilidad', 'Estado Evento',
           'Usuario_Creador', 'Usuario_Modificacion', 'Trafos Afectados',
           'Fecha creación Evento', 'Fecha modificación Evento', 'Borrado', 'IUA',
           'DURACION_min', 'REPORTE_MM', 'REPORTE_DD', 'CausasNoReportado',
           'ESTADO_VALIDACION']
    cols_to_drop = []
    for target in base_targets:
        match = next((c for c in QA_TFDDREGISTRO.columns if c.strip() == target.strip()), None)
        if match:
            cols_to_drop.append(match)
    if cols_to_drop:
        QA_TFDDREGISTRO.drop(cols_to_drop, axis = 1, inplace = True, errors='ignore')
    print(f"  → Columnas originales eliminadas correctamente ({len(cols_to_drop)} columnas)\n")

    #Assignment of values corresponding to the causes CREG (like a BUSCARV on Excel)
    print("Asignando FDD_CAUSA_CREG (lookup en QA_TFDDCAUSAS_SP7)...")
    QA_TFDDREGISTRO['FDD_CAUSA_CREG'] = QA_TFDDREGISTRO['FDD_CAUSA'].map(QA_TFDDCAUSAS_SP7.set_index('FDD_CAUSA_SP7')['FDD_CAUSA_CREG']).fillna('#N/A')
    print("  → FDD_CAUSA_CREG OK\n")

    #Assignment of values corresponding to the causes SSPD (like a BUSCARV on Excel)
    print("Asignando FDD_CAUSA_SSPD (lookup en QA_TFDDCAUSAS_SP7)...")
    QA_TFDDREGISTRO['FDD_CAUSA_SSPD'] = QA_TFDDREGISTRO['FDD_CAUSA'].map(QA_TFDDCAUSAS_SP7.set_index('FDD_CAUSA_SP7')['FDD_CAUSA_SSPD']).fillna('0')
    print("  → FDD_CAUSA_SSPD OK\n")

    #Assignment of values corresponding to the exclusions (like a BUSCARV on Excel), first convert the values in unique option
    print("Asignando FDD_EXCLUSION (lookup en QA_TFDDCAUSAS_OMS)...")
    mapping = (
        QA_TFDDCAUSAS_OMS.groupby('FDC_CAUSA_015')['FDC_EXCLUSION']
        .apply(lambda x: '; '.join(map(str, x.unique())))
    )
    QA_TFDDREGISTRO['FDD_EXCLUSION'] = QA_TFDDREGISTRO['FDD_CAUSA_CREG'].map(mapping).fillna('#N/A')

    na_count = (QA_TFDDREGISTRO['FDD_EXCLUSION'] == '#N/A').sum()
    print(f"  → FDD_EXCLUSION OK — Total filas con '#N/A': {na_count}\n")

    #Assingnment of values corresponding to the FDD_PERIODO_TC1 column
    print("Calculando FDD_PERIODO_TC1 (mes más frecuente en FDD_FINICIAL)...")
    QA_TFDDREGISTRO['MES_INICIAL'] = QA_TFDDREGISTRO['FDD_FINICIAL'].dt.to_period('M').dt.to_timestamp()
    conteo_meses = QA_TFDDREGISTRO['MES_INICIAL'].value_counts()
    mes_mas_frecuente = conteo_meses.idxmax()
    QA_TFDDREGISTRO['FDD_PERIODO_TC1'] = mes_mas_frecuente.strftime('%Y%m')
    QA_TFDDREGISTRO.drop(columns=['MES_INICIAL'], inplace=True)
    print(f"  → FDD_PERIODO_TC1 = {mes_mas_frecuente.strftime('%Y%m')} OK\n")

    #Assing to the FDD_USUARIOAP column the values corresponding to the public lighting
    print("Asignando FDD_USUARIOAP (alumbrado público desde QA_TTT2_REGISTRO)...")
    mapping_AP = (
        QA_TTT2_REGISTRO.groupby('TT2_CODIGOELEMENTO')['TT2_CODE_CALP']
        .apply(lambda x: '; '.join(map(str, x.unique())))
    )
    QA_TFDDREGISTRO['FDD_USUARIOAP'] = QA_TFDDREGISTRO['FDD_CODIGOELEMENTO'].map(mapping_AP).fillna('N')

    QA_TFDDREGISTRO['FDD_USUARIOAP'] = QA_TFDDREGISTRO['FDD_USUARIOAP'].apply(
        lambda x: 'S' if isinstance(x, str) and x.startswith('CALP') else 'N'
    )
    print("  → FDD_USUARIOAP OK\n")

    #Assign to the FDD_CAUSA_SSPD column the cause 1 corresponding to the events minors than 3 minutes
    print("Ajustando FDD_CAUSA_SSPD para eventos menores a 3 minutos...")
    duracion_min = (QA_TFDDREGISTRO["FDD_FFINAL"] - QA_TFDDREGISTRO["FDD_FINICIAL"]).dt.total_seconds() / 60
    condicion = (QA_TFDDREGISTRO["FDD_CAUSA_SSPD"] == '0') & (duracion_min <= 3)
    QA_TFDDREGISTRO.loc[condicion, "FDD_CAUSA_SSPD"] = '1'
    print(f"  → Filas ajustadas a causa SSPD=1: {condicion.sum()}\n")

    #Remove duplicates
    print("Eliminando duplicados...")
    before = len(QA_TFDDREGISTRO)
    QA_TFDDREGISTRO.drop_duplicates(inplace=True)
    print(f"  → Duplicados eliminados: {before - len(QA_TFDDREGISTRO)} — Filas resultantes: {len(QA_TFDDREGISTRO)}\n")

    # Crear el DataFrame QA_TFDDREGISTRO_ELIMINAR con los registros donde FDD_TIPOAJUSTE == 3
    QA_TFDDREGISTRO_ELIMINAR = QA_TFDDREGISTRO[QA_TFDDREGISTRO['FDD_TIPOAJUSTE'] == '3'].copy()

    # Crear el DataFrame QA_TFDDREGISTRO_AGREGAR con el resto de registros
    QA_TFDDREGISTRO_AGREGAR = QA_TFDDREGISTRO[QA_TFDDREGISTRO['FDD_TIPOAJUSTE'] != '3'].copy()

    print(f"Registros a AGREGAR: {len(QA_TFDDREGISTRO_AGREGAR)} — Registros a ELIMINAR: {len(QA_TFDDREGISTRO_ELIMINAR)}\n")

    #Connecting to the database for extract the datasets from database itself
    #Perform the insertion into database on the QA_TFDDREGISTRO dataset that has been created

    # Obtener el periodo desde el DataFrame
    if 'FDD_PERIODO_TC1' not in QA_TFDDREGISTRO.columns:
        raise ValueError("El DataFrame no contiene la columna 'FDD_PERIODO_TC1'")

    # Extraer el periodo (asume que todos los valores son iguales)
    periodo = QA_TFDDREGISTRO['FDD_PERIODO_TC1'].iloc[0]
    print(f"Período de inserción: {periodo}\n")

    from conexion import open_conexion
    from sqlalchemy import text

    # Abre la conexión
    print("Abriendo conexión a la base de datos...")
    conn, engine = open_conexion()

    # Consultar si ya existen datos para ese periodo
    count_result = conn.execute(
        text(f"SELECT COUNT(1) FROM QA_TFDDREGISTRO WHERE FDD_PERIODO_TC1 = :periodo"),
        {"periodo": periodo}
    ).scalar()

    if count_result > 0:
        print(f"⚠️ Ya existen {count_result} registros con FDD_PERIODO_TC1 = {periodo}, se eliminarán...")
        conn.execute(
            text(f"DELETE FROM QA_TFDDREGISTRO WHERE FDD_PERIODO_TC1 = :periodo"),
            {"periodo": periodo}
        )
        conn.commit()

        conn.execute(
            text(f"DELETE FROM QA_TFDDELIMINADOS WHERE FDD_PERIODO_TC1 = :periodo"),
            {"periodo": periodo}
        )
        conn.commit()

        print("🗑️ Registros anteriores eliminados.")

    # Normalizar tipos antes de insertar para evitar errores del driver con datetimes
    print("Normalizando tipos para inserción...")
    def _normalize(df):
        df = df.copy()
        for col in df.columns:
            if pandas.api.types.is_datetime64_any_dtype(df[col]):
                # Convertir a Python datetime nativo (NaT → None)
                df[col] = df[col].apply(lambda x: x.to_pydatetime() if pandas.notna(x) else None)
            elif df[col].dtype == 'object':
                # Para columnas de texto, rellenar NaN con cadena vacía
                df[col] = df[col].fillna('').astype(str)
        # Mantener columnas en MAYÚSCULAS para Oracle (sin comillas dobles)
        df.columns = [c.upper() for c in df.columns]
        return df

    def _insert_df(conn, table_name, df, chunk_size=500):
        """Inserta un DataFrame en Oracle usando INSERT con parámetros nombrados sin comillas."""
        cols = list(df.columns)
        col_list   = ', '.join(cols)                     # FDD_CODIGOEVENTO, FDD_FINICIAL, ...
        param_list = ', '.join([f':{c}' for c in cols]) # :FDD_CODIGOEVENTO, :FDD_FINICIAL, ...
        sql = text(f"INSERT INTO {table_name} ({col_list}) VALUES ({param_list})")
        rows = df.to_dict(orient='records')
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i+chunk_size]
            conn.execute(sql, chunk)
            conn.commit()
            print(f"  → Insertados {min(i+chunk_size, len(rows))}/{len(rows)} registros en {table_name}")

    QA_TFDDREGISTRO_AGREGAR_DB  = _normalize(QA_TFDDREGISTRO_AGREGAR)
    QA_TFDDREGISTRO_ELIMINAR_DB = _normalize(QA_TFDDREGISTRO_ELIMINAR)
    print("  → DataFrames normalizados (datetime → Python datetime, NaT → None, columnas en MAYÚSCULAS)")

    # Insertar nuevos datos

    if engine is not None:
        try:
            print("Insertando en tabla QA_TFDDREGISTRO...")
            _insert_df(conn, 'QA_TFDDREGISTRO', QA_TFDDREGISTRO_AGREGAR_DB)
            print(f"  → {len(QA_TFDDREGISTRO_AGREGAR_DB)} registros insertados en QA_TFDDREGISTRO")

            if not QA_TFDDREGISTRO_ELIMINAR_DB.empty:
                print("Insertando en tabla QA_TFDDELIMINADOS...")
                _insert_df(conn, 'QA_TFDDELIMINADOS', QA_TFDDREGISTRO_ELIMINAR_DB)
                print(f"  → {len(QA_TFDDREGISTRO_ELIMINAR_DB)} registros insertados en QA_TFDDELIMINADOS")
            else:
                print("  → No hay registros para QA_TFDDELIMINADOS")

            print("✅ Datos insertados correctamente en QA_TFDDREGISTRO")
        except Exception as e:
            print("❌ Error al insertar en base de datos:", e)
            print("Columnas y tipos:")
            print(QA_TFDDREGISTRO_AGREGAR_DB.dtypes)
            print("Primer registro a insertar:")
            if not QA_TFDDREGISTRO_AGREGAR_DB.empty:
                print(QA_TFDDREGISTRO_AGREGAR_DB.iloc[0].to_dict())
            raise
        finally:
            conn.close()
    else:
        print("❌ No se pudo establecer conexión con la base de datos")

    QA_TFDDREGISTRO.to_csv(os.path.join(ruta_salida, 'QA_TFDDREGISTRO.csv'), index=False)
    print("✅ FINALIZADO")

    #show(QA_TFDDREGISTRO)
