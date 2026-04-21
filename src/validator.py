import subprocess
import sys
import importlib
import os
import shutil
import tempfile
import logging
import warnings

# from pandasgui import show  # No necesario en entorno web
import pandas
from pandas import to_datetime
# import sweetviz as sv  # No necesario en entorno web
# Usar web_utils.messagebox en lugar de tkinter.messagebox para compatibilidad web
try:
    from web_utils import messagebox
except ImportError:
    # Fallback a tkinter si web_utils no está disponible
    from tkinter import messagebox
import tempfile
from multiprocessing import Process

# def lanzar_pandasgui(path_csv):  # No necesario en entorno web
#     df = pandas.read_csv(path_csv, dtype=str)
#     show(df)

def _guardar_csv_via_temp(df, ruta_destino):
    """Escribe el DataFrame en un archivo temporal local y luego lo mueve al destino.
    Evita la escritura lenta fila-por-fila directamente sobre rutas de red/DFS."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv',
                                     encoding='utf-8', newline='') as tmp:
        tmp_path = tmp.name
    try:
        print(f"  ℹ️ Escribiendo en temporal local: {tmp_path}")
        df.to_csv(tmp_path, index=False)
        print(f"  ℹ️ Moviendo al destino: {ruta_destino}")
        shutil.move(tmp_path, ruta_destino)
        print(f"  ✅ Archivo guardado correctamente")
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

def parsear_fecha_flexible(serie: pandas.Series) -> pandas.Series:
    """
    Parsea fechas con o sin milisegundos usando fillna() para compatibilidad con pandas 2.x.
    Intenta primero con microsegundos, luego sin ellos y finalmente inferencia automática.
    """
    # Intento 1: con microsegundos  -> "01/03/2026 10:30:00.000000"
    resultado = pandas.to_datetime(serie, format="%d/%m/%Y %H:%M:%S.%f", errors="coerce")

    # Intento 2: sin microsegundos -> "01/03/2026 10:30:00"
    if resultado.isna().any():
        print(f"  ⚠️ '{serie.name}': {resultado.isna().sum()} fechas sin microsegundos, reintentando...")
        resultado = resultado.fillna(
            pandas.to_datetime(serie, format="%d/%m/%Y %H:%M:%S", errors="coerce")
        )

    # Intento 3: inferencia automática (dayfirst) para formatos restantes
    if resultado.isna().any():
        print(f"  ⚠️ '{serie.name}': {resultado.isna().sum()} fechas con formato desconocido, usando inferencia...")
        resultado = resultado.fillna(
            pandas.to_datetime(serie, errors="coerce", dayfirst=True)
        )

    # Diagnóstico final
    nulos_finales = resultado.isna().sum()
    if nulos_finales > 0:
        print(f"  ❌ '{serie.name}': {nulos_finales} fechas no pudieron parsearse:")
        print(f"     {serie[resultado.isna()].unique()[:5]}")
    else:
        print(f"  ✅ '{serie.name}': todas las fechas parseadas correctamente")

    return resultado


def _normalizar_columnas_sp7(df: pandas.DataFrame) -> pandas.DataFrame:
    """Normaliza nombres de columnas SP7 para soportar esquemas API y CSV local."""
    columnas_actuales = list(df.columns)
    columnas_upper = {c.upper(): c for c in columnas_actuales}

    alias_objetivo = {
        "Fecha_Desenergizacion": ["Fecha_Desenergizacion", "FECHA_OPEN"],
        "Fecha_Energizacion": ["Fecha_Energizacion", "FECHA_CLOSE"],
        "Maniobra Apertura": ["Maniobra Apertura", "MANIOBRA_OPEN"],
        "Borrado": ["Borrado", "BORRADO"],
        "Trafos Afectados": ["Trafos Afectados", "CANTIDAD_TRAFOS_AFECTADOS"],
        "Clientes": ["Clientes", "CLIENTES", "Clientes "],
        "Usuario_Creador": ["Usuario_Creador", "USR_CREACION"],
        "Usuario_Modificacion": ["Usuario_Modificacion", "USUARIO"],
        "Nodo": ["Nodo", "NODO"],
        "Tipo Elemento": ["Tipo Elemento", "TIPO_ELEMENTO"],
    }

    renombres = {}
    for canonica, aliases in alias_objetivo.items():
        if canonica in df.columns:
            continue
        for alias in aliases:
            encontrada = columnas_upper.get(alias.upper())
            if encontrada:
                renombres[encontrada] = canonica
                break

    if renombres:
        df = df.rename(columns=renombres)
        print(f"  ℹ️ Columnas normalizadas en SP7: {renombres}")

    return df


def validar_CONSOLIDADO_SP7():
    from loader import get_data_dir, get_resultados_dir
    ruta_salida = get_data_dir()
    ruta_resultados = get_resultados_dir()
    os.makedirs(ruta_resultados, exist_ok=True)

    print("crear el dataframe REPORTE_SP7")
    from loader import cargar_csv
    REPORTE_SP7 = cargar_csv("CONSOLIDADO_SP7", True)
    REPORTE_SP7.columns = REPORTE_SP7.columns.str.strip()  # eliminar espacios y saltos de línea
    REPORTE_SP7 = _normalizar_columnas_sp7(REPORTE_SP7)
    CONSOLIDADO_QE = cargar_csv("CONSOLIDADO_QE", True)

    print("agregar columna DURACION para la duracion entre los intervalos de tiempo de los eventos")
    print("  Parseando Fecha_Desenergizacion...")
    REPORTE_SP7["Fecha_Desenergizacion"] = parsear_fecha_flexible(REPORTE_SP7["Fecha_Desenergizacion"])
    print("  Parseando Fecha_Energizacion...")
    REPORTE_SP7["Fecha_Energizacion"] = parsear_fecha_flexible(REPORTE_SP7["Fecha_Energizacion"])

    REPORTE_SP7["DURACION_min"] = (
        REPORTE_SP7["Fecha_Energizacion"] - REPORTE_SP7["Fecha_Desenergizacion"]
    ).dt.total_seconds() / 60

    nulos_duracion = REPORTE_SP7["DURACION_min"].isna().sum()
    print(f"  ℹ️ DURACION_min calculada — registros con NaT: {nulos_duracion}")

    print("Crear dataframe del REPORTE MENSUAL")
    REPORTE_MM = cargar_csv("CENS_RMTA", False)

    print("Agregar nombre de columnas al df REPORTE_MM")
    REPORTE_MM.columns = ["CODIGO_EVENTO", "FECHA_INI", "FECHA_FIN", "CODIGO_ELEMENTO", "TIPO_ELEMENTO",
                              "CAUSA_CREG", "ESTADO_EVENTO", "ZNI", "AG", "AP", "TIPO_AJUSTE", "COD_RADICADO"]

    print("Agregar llave al df REPORTE_MM (KEY)")
    REPORTE_MM.insert(0, "KEY",
                           REPORTE_MM["CODIGO_EVENTO"].astype(str) +
                           "-" +
                           REPORTE_MM["CODIGO_ELEMENTO"].astype(str)
                           )  # agrega una columna KEY

    print("Traer la columna TIPO_AJUSTE del df REPORTE_MM al df REPORTE_SP7")
    REPORTE_SP7 = REPORTE_SP7.merge(
        REPORTE_MM[["KEY", "TIPO_AJUSTE"]],
        on="KEY",
        how="left"
    )

    print("Reemplazar valores nulos en la columna TIPO_AJUSTE con '0'")
    REPORTE_SP7["TIPO_AJUSTE"] = REPORTE_SP7["TIPO_AJUSTE"].fillna('0')

    print("Cambiar el nombre de la columna TIPO_AJUSTE a REPORTE_MM")
    REPORTE_SP7.rename(columns={"TIPO_AJUSTE": "REPORTE_MM"}, inplace=True)

    print("Traer marca de reporte diario al df REPORTE_SP7 desde CONSOLIDADO_QE")
    REPORTE_SP7 = REPORTE_SP7.merge(
        CONSOLIDADO_QE[["KEY", "TIPO_ELEMENTO"]].rename(columns={"TIPO_ELEMENTO": "TIPO_ELEMENTO_QE"}),
        on="KEY",
        how="left"
    )

    print("Reemplazar valores nulos en la columna TIPO_ELEMENTO_QE con '0'")
    REPORTE_SP7["TIPO_ELEMENTO_QE"] = REPORTE_SP7["TIPO_ELEMENTO_QE"].fillna('0')

    print("Cambiar el nombre de la columna TIPO_ELEMENTO_QE a REPORTE_DD")
    REPORTE_SP7.rename(columns={"TIPO_ELEMENTO_QE": "REPORTE_DD"}, inplace=True)

    print("Crear df NO_REPORTADOS")
    NO_REPORTADOS = cargar_csv("Noreportados", True)

    print("Agregar llave al df NO_REPORTADOS (KEY)")
    NO_REPORTADOS.insert(0, "KEY",
                           NO_REPORTADOS["CodigoEvento"].astype(str) +
                           "-" +
                           NO_REPORTADOS["CodigoElemento"].astype(str)
                           )  # agrega una columna KEY

    print("Traer marca de NO_REPORTADOS al df REPORTE_SP7 desde NO_REPORTADOS")
    REPORTE_SP7 = REPORTE_SP7.merge(
        NO_REPORTADOS[["KEY", "CausasNoReportado"]],
        on="KEY",
        how="left"
    )

    print("Verificacion de validación existente")

    if "ESTADO_VALIDACION" in REPORTE_SP7.columns:
        print("Columna encontrada. Procediendo con el análisis...")
        continuar = messagebox.askyesno("Validación SP7", "¿Ya existe una validacíon, "
                                                        "desea consultarlos?")
        if not continuar:
            print("🚫 Proceso cancelado")
            return

        print("Cantidad de registros: ", len(REPORTE_SP7))

        # Columnas base siempre presentes
        cols_vista = ["ESTADO_VALIDACION"]
        # Columnas opcionales — solo se incluyen si existen en el df
        cols_opcionales = [
            "Maniobra Apertura",
            "Usuario_Creador",
            "Usuario_Modificacion",
            "Borrado",
            "REPORTE_MM",
            "REPORTE_DD",
            "CausasNoReportado"
        ]
        cols_vista += [c for c in cols_opcionales if c in REPORTE_SP7.columns]
        vista_SP7 = REPORTE_SP7[cols_vista]
        # report = sv.analyze(vista_SP7)  # No necesario en web
        # report.show_html("reporte_df.html")  # No necesario en web

        PENDIENTES_NA = REPORTE_SP7[REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"]
        warnings.simplefilter(action='ignore', category=FutureWarning)

        # temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)  # No necesario en web
        # PENDIENTES_NA.to_csv(temp_file.name, index=False)  # No necesario en web
        # Process(target=lanzar_pandasgui, args=(temp_file.name,)).start()  # No necesario en web
        return

    print("Validacion SP7")
    print("Agregar columna de VALIDACION")
    REPORTE_SP7["ESTADO_VALIDACION"] = "sin definir"

    print("Marcar los eventos reportados en el formato diario")
    REPORTE_SP7.loc[
        (REPORTE_SP7["REPORTE_DD"] == '1') & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Reporte DD" #valor a colocar

    print("Marcar los eventos agregados en el foramto ajuste")
    REPORTE_SP7.loc[
        (REPORTE_SP7["REPORTE_MM"] == '1') & (REPORTE_SP7["REPORTE_DD"] == '0'), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Agregado MM" #valor a colocar

    print("Marcar los eventos modificados en el foramto ajuste")
    REPORTE_SP7.loc[
        (REPORTE_SP7["REPORTE_MM"] == '2') & (REPORTE_SP7["REPORTE_DD"] == '1'), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Modificado MM" #valor a colocar

    print("Marcar los eventos modificados mes anterior en el foramto ajuste")
    REPORTE_SP7.loc[
        (REPORTE_SP7["REPORTE_MM"] == '2') & (REPORTE_SP7["REPORTE_DD"] == '0'), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Modificado MM - mes anterior" #valor a colocar

    print("Marcar los eventos eliminados en el foramto ajuste")
    REPORTE_SP7.loc[
        (REPORTE_SP7["REPORTE_MM"] == '3') & (REPORTE_SP7["REPORTE_DD"] == '1'), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Eliminado MM" #valor a colocar

    print("Marcar los eventos de efecto borrado")
    print(f"Columnas disponibles en REPORTE_SP7: {REPORTE_SP7.columns.tolist()}")
    REPORTE_SP7.loc[
        (REPORTE_SP7["Borrado"] == "Y") & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Efecto borrado"

    print("Marcar los eventos de efecto cero")
    REPORTE_SP7.loc[
        (REPORTE_SP7["Trafos Afectados"] == '0') & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Evento sin efecto"

    print("Marcar los eventos sin causa asignada")
    REPORTE_SP7.loc[
        (REPORTE_SP7["Maniobra Apertura"].isna()) & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Evento sin causa"

    print("Marcar los eventos con duracion cero")
    REPORTE_SP7.loc[
        (REPORTE_SP7["DURACION_min"] == 0) & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Evento con duración cero"

    print("Convertir las fechas en el DataFrame original — ya son datetime desde parsear_fecha_flexible")

    print("Filtrar registros donde el mes y año coinciden")
    coinciden = REPORTE_SP7[
        (REPORTE_SP7["Fecha_Desenergizacion"].dt.month == REPORTE_SP7["Fecha_Energizacion"].dt.month) &
        (REPORTE_SP7["Fecha_Desenergizacion"].dt.year == REPORTE_SP7["Fecha_Energizacion"].dt.year)
        ]

    print("Extraer año y mes de la fecha (puedes usar cualquiera de las dos porque coinciden")
    month_min = coinciden["Fecha_Desenergizacion"].dt.month.min()
    print('month_min: ' , month_min)
    month_max = coinciden["Fecha_Desenergizacion"].dt.month.max()
    print('month_max: ' , month_max)
    year = coinciden["Fecha_Desenergizacion"].dt.year.min()
    print('year: ' , year)

    if month_min == 1 and month_max == 12:
        month = 12
    else:
        month = month_min
    print('month: ', month)

    print("Marcar los eventos de solo cierres")
    REPORTE_SP7.loc[
        (REPORTE_SP7["Fecha_Desenergizacion"].dt.month <= month - 1) &
        (REPORTE_SP7["Fecha_Desenergizacion"].dt.year <= year) &
        (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Cierres de eventos del mes anterior"

    print("Visualiza de forma completa en pandasgui las fechas sin omitir ceros")
    REPORTE_SP7["Fecha_Desenergizacion"] = REPORTE_SP7["Fecha_Desenergizacion"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    REPORTE_SP7["Fecha_Energizacion"] = REPORTE_SP7["Fecha_Energizacion"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    print("Reemplazar valores nulos en la columna CausasNoReportado con '0'")
    REPORTE_SP7["CausasNoReportado"] = REPORTE_SP7["CausasNoReportado"].fillna('NA')

    print("Marcar los registros de no_reportados en la columna ESATDO_VALIDACION")
    REPORTE_SP7.loc[
        (REPORTE_SP7["CausasNoReportado"] != 'NA') & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = REPORTE_SP7["CausasNoReportado"] #valor a colocar

    print("Marcar los registros de con cero usuarios en el transformador afectado - ESATDO_VALIDACION")
    REPORTE_SP7.loc[
        (REPORTE_SP7["Clientes"] == '0') & (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Transformador sin usuarios"

    print("Encontrar maxima fecha de inicio de reporte del CONSOLIDADO_QE")

    print("Se rectifica que la FECHA_INI sea datetime")
    CONSOLIDADO_QE['FECHA_INI'] = pandas.to_datetime(CONSOLIDADO_QE['FECHA_INI'], errors='coerce', dayfirst=True)

    print("Crear columna solo con fecha (pero conservar tipo datetime)")
    CONSOLIDADO_QE['Solo_Fecha'] = CONSOLIDADO_QE['FECHA_INI'].dt.normalize()

    print("Eliminar fechas nulas antes de buscar la máxima")
    fechas_validas = CONSOLIDADO_QE['Solo_Fecha'].dropna()

    print("Obtener fecha máxima (tipo datetime sin hora)")
    fecha_maxima = fechas_validas.max()

    print("Fecha máxima (sólo día):", fecha_maxima)

    print("Marcar los eventos que se encuentren despues de la fecha maxima reportada en CONSOLIDADO_SP7")
    print("Duplica la columna Fecha_Desenergizacion")
    REPORTE_SP7["Fecha_Desenergizacion_2"] = REPORTE_SP7["Fecha_Desenergizacion"]

    REPORTE_SP7["Fecha_Desenergizacion_2"] = pandas.to_datetime(REPORTE_SP7["Fecha_Desenergizacion_2"], errors="coerce")#, dayfirst=True)

    cantidad_nat = REPORTE_SP7["Fecha_Desenergizacion_2"].isna().sum()
    print(f"Cantidad de valores NaT en 'Fecha_Desenergizacion_2': {cantidad_nat}")

    print("****Fecha máxima:", fecha_maxima)
    print("****Fecha máxima + 1 día:", fecha_maxima + pandas.Timedelta(days=1))

    print("Fecha mayor a fecha máxima + 1 día:",
          (REPORTE_SP7["Fecha_Desenergizacion_2"] > (fecha_maxima + pandas.Timedelta(days=1))).sum())

    print('ESTADO_VALIDACION == "sin definir":',
          (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir").sum())

    print("Fecha máxima en REPORTE_SP7:", REPORTE_SP7["Fecha_Desenergizacion_2"].max())

    print(REPORTE_SP7["Fecha_Desenergizacion_2"].head(10))

    fechas_invalidas = REPORTE_SP7[
        ~REPORTE_SP7["Fecha_Desenergizacion_2"].astype(str).str.match(r"\d{2}/\d{2}/\d{4}")  # ajusta al formato real
    ]

    print("Fechas potencialmente mal formateadas:")
    print(fechas_invalidas["Fecha_Desenergizacion_2"].unique())
    
    REPORTE_SP7.loc[
        (REPORTE_SP7["Fecha_Desenergizacion_2"] > (fecha_maxima )) & #+ pandas.Timedelta(days=1)
        (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"),
        "ESTADO_VALIDACION"
    ] = "Eventos aun no reportados - 36 hrs" 

    condicion = (
            (REPORTE_SP7["Fecha_Desenergizacion_2"] > (fecha_maxima + pandas.Timedelta(days=1))) &
            (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir")
            )

    filas_afectadas = condicion.sum()
    print("Filas que serán actualizadas - 36 horas:", filas_afectadas)

    REPORTE_SP7.loc[condicion, "ESTADO_VALIDACION"] = "Eventos aun no reportados - 36 hrs"

    print("Eliminacion de la columna Fecha_Desenergizacion_2")
    REPORTE_SP7 = REPORTE_SP7.drop(columns=["Fecha_Desenergizacion_2"])

    print("Crear CONSOLIDADO_SP7 en csv")
    _guardar_csv_via_temp(REPORTE_SP7, os.path.join(ruta_resultados, 'CONSOLIDADO_SP7.csv'))

    print("Mostar por consola cantidad pendiente de validacion")
    cantidad = (REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir").sum()
    print(f"⚠️ Existen {cantidad} registros pendientes por validar.")


    print(REPORTE_SP7.columns)
    # print("Silenciar INFO de pandasgui")  # No necesario en web
    # logging.getLogger('pandasgui').setLevel(logging.CRITICAL)  # No necesario en web
    print("Silenciar FutureWarnings")
    warnings.simplefilter(action='ignore', category=FutureWarning)

    print(f"✅ Data CONSOLIDADO_SP7 validada!")

    print("Cantidad de registros: ", len(REPORTE_SP7))
    print("Selecciona los campos a mostrar graficamente en el navegador")
    vista_SP7 = REPORTE_SP7[["ESTADO_VALIDACION"
        #,"COD_APERTURA"
        #,"USR_CREACION"
        #,"USUARIO"
        #,"BORRADO"
        #,"REPORTE_MM"
        #,"REPORTE_DD"
        #,"CausasNoReportado"
        ]]
    # report = sv.analyze(vista_SP7)  # No necesario en web
    # report.show_html("reporte_df.html")  # No necesario en web

    PENDIENTES_NA = REPORTE_SP7[REPORTE_SP7["ESTADO_VALIDACION"] == "sin definir"]
    warnings.simplefilter(action='ignore', category=FutureWarning)

    # temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)  # No necesario en web
    # PENDIENTES_NA.to_csv(temp_file.name, index=False)  # No necesario en web
    # Process(target=lanzar_pandasgui, args=(temp_file.name,)).start()  # No necesario en web

def validar_CONSOLIDADO_QE():
    print("Validacion QEnergía")
    from loader import get_data_dir, get_resultados_dir
    ruta_salida = get_data_dir()
    ruta_resultados = get_resultados_dir()
    os.makedirs(ruta_resultados, exist_ok=True)

    print("crear el dataframe CONSOLIDADO_SP7 y CONSOLIDADO_QE")
    from loader import cargar_csv
    CONSOLIDADO_SP7 = cargar_csv("CONSOLIDADO_SP7", True)
    CONSOLIDADO_QE = cargar_csv("CONSOLIDADO_QE", True)
    CONSOLIDADO_SP7.columns = CONSOLIDADO_SP7.columns.str.strip()
    CONSOLIDADO_QE.columns = CONSOLIDADO_QE.columns.str.strip()
    CONSOLIDADO_SP7 = _normalizar_columnas_sp7(CONSOLIDADO_SP7)

    if "KEY" not in CONSOLIDADO_SP7.columns and {"Maniobra Apertura", "IUA"}.issubset(CONSOLIDADO_SP7.columns):
        print("  ⚠️ CONSOLIDADO_SP7 no tiene KEY; se construye desde Maniobra Apertura + IUA")
        CONSOLIDADO_SP7["KEY"] = (
            CONSOLIDADO_SP7["Maniobra Apertura"].astype(str) + "-" + CONSOLIDADO_SP7["IUA"].astype(str)
        )

    if "KEY" not in CONSOLIDADO_QE.columns:
        raise KeyError(f"CONSOLIDADO_QE no contiene KEY. Columnas: {list(CONSOLIDADO_QE.columns)}")

    if "ESTADO_VALIDACION" not in CONSOLIDADO_SP7.columns:
        print("  ⚠️ CONSOLIDADO_SP7 no tiene ESTADO_VALIDACION; se usará 'sin definir' para todos")
        CONSOLIDADO_SP7["ESTADO_VALIDACION"] = "sin definir"

    if "Fecha_Desenergizacion" not in CONSOLIDADO_SP7.columns or "Fecha_Energizacion" not in CONSOLIDADO_SP7.columns:
        raise KeyError(
            "CONSOLIDADO_SP7 no contiene columnas de fechas esperadas. "
            f"Columnas disponibles: {list(CONSOLIDADO_SP7.columns)}"
        )

    print("Verificacion de validación existente")

    """if "ESTADO_VALIDACION" in CONSOLIDADO_QE.columns:
        print("Columna encontrada. Procediendo con el análisis...")
        continuar = messagebox.askyesno("Validación QE", "¿Ya existe una validacíon, "
                                                          "desea consultarlos?")
        if not continuar:
            print("🚫 Proceso cancelado")
            return

        print("Cantidad de registros: ", len(CONSOLIDADO_QE))
        # report = sv.analyze(CONSOLIDADO_QE)  # No necesario en web
        # report.show_html("reporte_df.html")  # No necesario en web

        PENDIENTES_NA = CONSOLIDADO_QE[CONSOLIDADO_QE["ESTADO_VALIDACION"] == "sin definir"]
        warnings.simplefilter(action='ignore', category=FutureWarning)

        # temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)  # No necesario en web
        # PENDIENTES_NA.to_csv(temp_file.name, index=False)  # No necesario en web
        # Process(target=lanzar_pandasgui, args=(temp_file.name,)).start()  # No necesario en web
        return
    """

    print("*******************************")
    print(CONSOLIDADO_QE["FECHA_INI"].astype(str).unique()[:1000])

    print("campos de la tabla CONSOLIDADO_QE: "
          "KEY"
          ",CODIGO_EVENTO"
          ",FECHA_INI"
          ",FECHA_FIN"
          ",CODIGO_ELEMENTO"
          ",TIPO_ELEMENTO"
          ",CAUSA_CREG"
          ",ESTADO_EVENTO"
          ",ZNI"
          ",AG"
          ",AP")

    print("Validacion QE")
    print("Agregar columna de VALIDACION")
    print("BUSCARV o LEFT OUTER JOIN para traer ESTADO_VALIDACION desde CONSOLIDADO_SP7 a CONSOLIDADO_QE")
    CONSOLIDADO_QE = CONSOLIDADO_QE.merge(
        CONSOLIDADO_SP7[["KEY", "ESTADO_VALIDACION"]],
        on="KEY",
        how="left"
    )
    print("Cambio de la columna de ESTADO_VALIDACION a ESTADO_REPORTE")
    CONSOLIDADO_QE.rename(columns={"ESTADO_VALIDACION" : "ESTADO_REPORTE"}, inplace=True)

    print("imprime las columnas existentes de CONSOLIDADO_QE")
    print(CONSOLIDADO_QE.columns)

    print("Los valores tomados de la columna ESTADO_VALIDACION son: ")
    print(CONSOLIDADO_QE["ESTADO_REPORTE"].unique())

    print("Devuelve los valores con la cantidad de ocurrencia de la columna ESTADO_REPORTE del df CONSOLIDADO_QE: ")
    print(CONSOLIDADO_QE["ESTADO_REPORTE"].value_counts())

    print("Los campos del CONSOLIDADO_SP7 son: ")
    print(CONSOLIDADO_SP7.columns)

    print("BUSCARV o LEFT OUTER JOIN para traer los campos FECHA_INI, FECHA_FIN, CAUSA_CREG desde "
          "CONSOLIDADO_SP7 hacia CONSOLIDADO_QE: ")
    CONSOLIDADO_QE = CONSOLIDADO_QE.merge(
        CONSOLIDADO_SP7[["KEY", "Fecha_Desenergizacion", "Fecha_Energizacion"]],
        on="KEY",
        how="left"
    )

    print("Mostrar los primeros 10 registros de CONSOLIDADO_QE despues de la operacion")
    print(CONSOLIDADO_QE.head(10))


    print("Creacion de la columna ESTADO_VALIDACION")
    CONSOLIDADO_QE["ESTADO_VALIDACION"] = "Sin definir"

    print(CONSOLIDADO_QE.head(10))

    print("Cantidad del campo ESTADO_VALIDACION: ")
    print(CONSOLIDADO_QE["ESTADO_VALIDACION"].value_counts())

    #comparacion de fechas para el campo de fecha de apertura
    print("Igualar las fechas de comparacion a un solo formato, teniendo en cuenta la naturaleza de cada una")

    CONSOLIDADO_QE["FECHA_INI"] = pandas.to_datetime(
        CONSOLIDADO_QE["FECHA_INI"], errors="coerce", dayfirst=False, format = "%d/%m/%Y %H:%M:%S.%f")

    print("genera un formato personalizdo al campo de fecha")
    CONSOLIDADO_QE["FECHA_INI"] = CONSOLIDADO_QE["FECHA_INI"].dt.strftime("%d/%m/%Y %H:%M:%S.%f")

    CONSOLIDADO_QE["Fecha_Desenergizacion"] = pandas.to_datetime(
        CONSOLIDADO_QE["Fecha_Desenergizacion"], errors="coerce", dayfirst=True)
    print("genera un formato personalizdo al campo de fecha")
    CONSOLIDADO_QE["Fecha_Desenergizacion"] = CONSOLIDADO_QE["Fecha_Desenergizacion"].dt.strftime("%d/%m/%Y %H:%M:%S.%f")

    pandas.set_option('display.max_rows', 100)
    print(CONSOLIDADO_QE[["FECHA_INI", "Fecha_Desenergizacion", "ESTADO_VALIDACION"]].head(100))


    CONSOLIDADO_QE.loc[
        (CONSOLIDADO_QE["ESTADO_REPORTE"] == "Reporte DD") &
        (CONSOLIDADO_QE["FECHA_INI"] != CONSOLIDADO_QE["Fecha_Desenergizacion"]
         ), #condiciones
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Diferencia en la fecha de apertura" #valor a colocar

    print(CONSOLIDADO_QE["ESTADO_VALIDACION"].value_counts())

    """
    #comparacion de fechas para el campo de fechas de cierre
    print("Comparar que las columnas de fechas sean exactamente las mismas para todos los eventos o registros que "
          "se encuentran con la casilla ESTADO_REPORTE = 'REPORTE DD'")

    CONSOLIDADO_QE["FECHA_FIN"] = pandas.to_datetime(
        CONSOLIDADO_QE["FECHA_FIN"], errors="coerce", dayfirst=False, format = "%d/%m/%Y %H:%M:%S.%f")

    CONSOLIDADO_QE["Fecha_Eenergizacion"] = pandas.to_datetime(
        CONSOLIDADO_QE["Fecha_Energizacion"], errors="coerce", dayfirst=True, format = "%d/%m/%Y %H:%M:%S.%f")

    pandas.set_option('display.max_rows', 1000)
    print(CONSOLIDADO_QE[["FECHA_FIN", "Fecha_Energizacion", "ESTADO_VALIDACION"]].head(1000))
    
    CONSOLIDADO_QE.loc[
        (CONSOLIDADO_QE["ESTADO_REPORTE"] == "Reporte DD") & (CONSOLIDADO_QE["FECHA_FIN"] != CONSOLIDADO_QE["Fecha_Energizacion"]), #condicion
        "ESTADO_VALIDACION" #columna a modificar
    ] = "Diferencia en la fecha de cierre" #valor a colocar
    
    print(CONSOLIDADO_QE["ESTADO_VALIDACION"].value_counts())
    

    print("Crear CONSOLIDADO_QE en csv")
    CONSOLIDADO_QE.to_csv(os.path.join(ruta_resultados, 'CONSOLIDADO_QE.csv'), index=False)

    print("Mostar por consola cantidad de errores de validacion")
    cantidad = (CONSOLIDADO_QE["ESTADO_VALIDACION"] != "sin definir").sum()
    print(f"⚠️ Existen {cantidad} registros con errores en la validación.")

    print(CONSOLIDADO_QE.columns)
    # print("Silenciar INFO de pandasgui")  # No necesario en web
    # logging.getLogger('pandasgui').setLevel(logging.CRITICAL)  # No necesario en web
    print("Silenciar FutureWarnings")
    warnings.simplefilter(action='ignore', category=FutureWarning)

    print(f"✅ Data CONSOLIDADO_QE validada!")

    print("Cantidad de registros: ", len(CONSOLIDADO_QE))
    # report = sv.analyze(CONSOLIDADO_QE)  # No necesario en web
    # report.show_html("reporte_df.html")  # No necesario en web

    PENDIENTES_NA = CONSOLIDADO_QE[CONSOLIDADO_QE["ESTADO_VALIDACION"] == "sin definir"]
    warnings.simplefilter(action='ignore', category=FutureWarning)

    # temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)  # No necesario en web
    # PENDIENTES_NA.to_csv(temp_file.name, index=False)  # No necesario en web
    # Process(target=lanzar_pandasgui, args=(temp_file.name,)).start()  # No necesario en web
    """

def validar_CONSOLIDADO_XM():
    print("validacion XM")