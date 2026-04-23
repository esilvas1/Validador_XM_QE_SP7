import pandas as pd
import os
import glob
import zipfile
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


def get_data_dir():
    env_data_dir = os.environ.get("DATA_DIR", "").strip()

    if os.name != "nt":
        env_data_dir_linux = os.environ.get("DATA_DIR_LINUX", "").strip()
        if env_data_dir_linux:
            env_data_dir = env_data_dir_linux

    if not env_data_dir:
        raise EnvironmentError(
            "DATA_DIR no configurado. Debe apuntar al DFS "
            "(en Linux puede usar DATA_DIR_LINUX como override)."
        )

    if os.name != "nt" and env_data_dir.startswith("\\\\"):
        raise FileNotFoundError(
            "DATA_DIR apunta a una ruta UNC de Windows en un entorno Linux. "
            "Monte el DFS en el servidor y configure DATA_DIR con ruta POSIX "
            "(ejemplo: /mnt/dfs/S0022/data)."
        )

    data_dir = os.path.abspath(env_data_dir)
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"DATA_DIR no existe o no es accesible: {data_dir}")

    return data_dir


def get_resultados_dir():
    return os.path.join(get_data_dir(), "Resultados")


def _get_reporte_mensual_dir():
    return os.path.join(get_data_dir(), "Reporte Mensual")


def _normalize_name(value):
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _find_reporte_mensual_csv(nombre_parcial):
    reporte_dir = _get_reporte_mensual_dir()
    patron = os.path.join(reporte_dir, "**", f"*{nombre_parcial}*.csv")
    return glob.glob(patron, recursive=True)


def _find_reporte_mensual_zip(nombre_parcial):
    reporte_dir = _get_reporte_mensual_dir()
    patron = os.path.join(reporte_dir, "**", f"*{nombre_parcial}*.zip")
    return glob.glob(patron, recursive=True)


def _find_resultados_csv(nombre_parcial):
    resultados_dir = get_resultados_dir()
    patron = os.path.join(resultados_dir, f"*{nombre_parcial}*.csv")
    return glob.glob(patron)


def _find_all_reporte_mensual_zips():
    reporte_dir = _get_reporte_mensual_dir()
    patron = os.path.join(reporte_dir, "**", "*.zip")
    return glob.glob(patron, recursive=True)


def _find_reporte_mensual_csv_anycase(nombre_parcial):
    reporte_dir = _get_reporte_mensual_dir()
    nombre_lower = nombre_parcial.lower()
    nombre_normalizado = _normalize_name(nombre_parcial)
    encontrados = []

    for root, _, files in os.walk(reporte_dir):
        for file_name in files:
            if not file_name.lower().endswith(".csv"):
                continue
            file_lower = file_name.lower()
            if nombre_lower in file_lower:
                encontrados.append(os.path.join(root, file_name))
                continue
            if nombre_normalizado and nombre_normalizado in _normalize_name(file_name):
                encontrados.append(os.path.join(root, file_name))

    return encontrados


def _read_csv_from_zip(zip_path, nombre_parcial, header_param):
    nombre_lower = nombre_parcial.lower()
    nombre_normalizado = _normalize_name(nombre_parcial)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        candidatos = [
            name for name in zip_ref.namelist()
            if name.lower().endswith(".csv") and (
                nombre_lower in name.lower() or nombre_normalizado in _normalize_name(name)
            )
        ]
        if not candidatos:
            raise FileNotFoundError(
                f"No se encontró CSV que contenga '{nombre_parcial}' dentro de {zip_path}"
            )
        if len(candidatos) > 1:
            raise FileExistsError(
                "Se encontraron múltiples CSV dentro del zip que coinciden con "
                f"'{nombre_parcial}':\n" + "\n".join(candidatos)
            )
        with zip_ref.open(candidatos[0]) as csv_file:
            return pd.read_csv(csv_file, encoding="utf-8", dtype=str, header=header_param)


def _read_csv_from_any_zip(zip_paths, nombre_parcial, header_param):
    nombre_lower = nombre_parcial.lower()
    nombre_normalizado = _normalize_name(nombre_parcial)
    matches = []

    for zip_path in zip_paths:
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for name in zip_ref.namelist():
                    if not name.lower().endswith(".csv"):
                        continue
                    name_lower = name.lower()
                    if nombre_lower in name_lower or nombre_normalizado in _normalize_name(name):
                        matches.append((zip_path, name))
        except zipfile.BadZipFile:
            continue

    if not matches:
        raise FileNotFoundError(
            f"No se encontró CSV que contenga '{nombre_parcial}' dentro de los ZIP de Reporte Mensual"
        )

    if len(matches) > 1:
        detalles = "\n".join([f"{zip_path} -> {name}" for zip_path, name in matches])
        raise FileExistsError(
            "Se encontraron múltiples CSV dentro de ZIP que coinciden con "
            f"'{nombre_parcial}':\n{detalles}"
        )

    zip_path, csv_name = matches[0]
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        with zip_ref.open(csv_name) as csv_file:
            return pd.read_csv(csv_file, encoding="utf-8", dtype=str, header=header_param)


def _find_in_dir_listdir(directorio, nombre_parcial):
    """Búsqueda robusta con os.listdir (funciona mejor en rutas UNC)."""
    nombre_lower = nombre_parcial.lower()
    nombre_norm = _normalize_name(nombre_parcial)
    encontrados = []
    try:
        if not os.path.isdir(directorio):
            print(f"  ⚠️ _find_in_dir_listdir: directorio NO existe: {directorio}")
            return []
        for fname in os.listdir(directorio):
            if not fname.lower().endswith(".csv"):
                continue
            if nombre_lower in fname.lower() or nombre_norm in _normalize_name(fname):
                encontrados.append(os.path.join(directorio, fname))
    except Exception as e:
        print(f"  ⚠️ _find_in_dir_listdir error listando '{directorio}': {e}")
    return encontrados


def cargar_csv(nombre_parcial, tiene_encabezado=True, solo_original=False):
    ruta_base = get_data_dir()
    patron_busqueda = os.path.join(ruta_base, f"*{nombre_parcial}*.csv")
    archivos_encontrados = glob.glob(patron_busqueda)

    print(f"🔍 cargar_csv('{nombre_parcial}') — ruta_base: {ruta_base}")
    print(f"   Patrón base: {patron_busqueda}  →  encontrados: {len(archivos_encontrados)}")

    reportes_especiales = {
        "Noreportados",
        "CENS_RMTA",
        "cens_TransformadoresReportados",
        "Consulta_Eventos_Cens_Apertura_Cierre",
    }

    resultados_especiales = {
        "CONSOLIDADO_QE",
        "CONSOLIDADO_SP7",
        "CONSOLIDADO_XM",
    }

    if not archivos_encontrados:
        # --- Búsqueda en Resultados (omitida si solo_original=True) ---
        if nombre_parcial in resultados_especiales and not solo_original:
            archivos_resultados = _find_resultados_csv(nombre_parcial)
            print(f"   Resultados: encontrados {len(archivos_resultados)}")
            if len(archivos_resultados) > 1:
                raise FileExistsError(
                    f"Se encontraron múltiples archivos que coinciden con '{nombre_parcial}':\n"
                    + "\n".join(archivos_resultados)
                )
            if len(archivos_resultados) == 1:
                archivos_encontrados = archivos_resultados

        # --- Búsqueda en Reporte Mensual ---
        if nombre_parcial in reportes_especiales and not archivos_encontrados:
            rm_dir = _get_reporte_mensual_dir()
            print(f"   Reporte Mensual dir: {rm_dir}")
            print(f"   ¿Existe?: {os.path.isdir(rm_dir)}")

            # 1) glob recursivo
            archivos_reporte = _find_reporte_mensual_csv(nombre_parcial)
            print(f"   glob CSV: {archivos_reporte}")
            if len(archivos_reporte) > 1:
                raise FileExistsError(
                    f"Se encontraron múltiples archivos que coinciden con '{nombre_parcial}':\n"
                    + "\n".join(archivos_reporte)
                )
            if len(archivos_reporte) == 1:
                archivos_encontrados = archivos_reporte

            # 2) os.walk case-insensitive
            if not archivos_encontrados:
                archivos_reporte_anycase = _find_reporte_mensual_csv_anycase(nombre_parcial)
                print(f"   os.walk anycase CSV: {archivos_reporte_anycase}")
                if len(archivos_reporte_anycase) > 1:
                    raise FileExistsError(
                        f"Se encontraron múltiples archivos que coinciden con '{nombre_parcial}':\n"
                        + "\n".join(archivos_reporte_anycase)
                    )
                if len(archivos_reporte_anycase) == 1:
                    archivos_encontrados = archivos_reporte_anycase

            # 3) os.listdir directo (robusto en UNC)
            if not archivos_encontrados:
                archivos_listdir = _find_in_dir_listdir(rm_dir, nombre_parcial)
                print(f"   os.listdir CSV: {archivos_listdir}")
                if len(archivos_listdir) > 1:
                    raise FileExistsError(
                        f"Se encontraron múltiples archivos que coinciden con '{nombre_parcial}':\n"
                        + "\n".join(archivos_listdir)
                    )
                if len(archivos_listdir) == 1:
                    archivos_encontrados = archivos_listdir

            # 4) Buscar dentro de ZIPs
            if not archivos_encontrados:
                archivos_zip = _find_reporte_mensual_zip(nombre_parcial)
                print(f"   glob ZIP por nombre: {archivos_zip}")
                if len(archivos_zip) > 1:
                    raise FileExistsError(
                        f"Se encontraron múltiples ZIP que coinciden con '{nombre_parcial}':\n"
                        + "\n".join(archivos_zip)
                    )
                if len(archivos_zip) == 1:
                    header_param = 0 if tiene_encabezado else None
                    return _read_csv_from_zip(archivos_zip[0], nombre_parcial, header_param)

                # 5) Escanear todos los ZIPs
                header_param = 0 if tiene_encabezado else None
                all_zips = _find_all_reporte_mensual_zips()
                print(f"   Todos los ZIPs en RM: {len(all_zips)}")
                if all_zips:
                    return _read_csv_from_any_zip(all_zips, nombre_parcial, header_param)

        # --- Búsqueda directa en ruta_base con os.listdir (fallback UNC) ---
        if not archivos_encontrados:
            archivos_base_listdir = _find_in_dir_listdir(ruta_base, nombre_parcial)
            print(f"   os.listdir base dir: {archivos_base_listdir}")
            if len(archivos_base_listdir) > 1:
                raise FileExistsError(
                    f"Se encontraron múltiples archivos que coinciden con '{nombre_parcial}':\n"
                    + "\n".join(archivos_base_listdir)
                )
            if len(archivos_base_listdir) == 1:
                archivos_encontrados = archivos_base_listdir

        # --- Búsqueda en ZIPs dentro de ruta_base y subdirectorios ---
        if not archivos_encontrados:
            header_param = 0 if tiene_encabezado else None
            zips_base_nombre = glob.glob(os.path.join(ruta_base, f"*{nombre_parcial}*.zip"))
            print(f"   ZIP por nombre en base dir: {zips_base_nombre}")
            if len(zips_base_nombre) == 1:
                return _read_csv_from_zip(zips_base_nombre[0], nombre_parcial, header_param)
            if len(zips_base_nombre) > 1:
                raise FileExistsError(
                    f"Se encontraron múltiples ZIPs que coinciden con '{nombre_parcial}':\n"
                    + "\n".join(zips_base_nombre)
                )
            # Escanear todos los ZIPs en ruta_base
            all_zips_base = glob.glob(os.path.join(ruta_base, "*.zip")) + glob.glob(os.path.join(ruta_base, "**", "*.zip"), recursive=True)
            all_zips_base = list(set(all_zips_base))
            print(f"   Todos los ZIPs en base dir: {len(all_zips_base)}")
            if all_zips_base:
                try:
                    return _read_csv_from_any_zip(all_zips_base, nombre_parcial, header_param)
                except FileNotFoundError:
                    pass

        if not archivos_encontrados:
            raise FileNotFoundError(
                f"No se encontró ningún archivo que contenga '{nombre_parcial}' en:\n"
                f"  - {ruta_base}\n"
                f"  - {_get_reporte_mensual_dir()}\n"
                f"  - {get_resultados_dir()}"
            )
    elif len(archivos_encontrados) > 1:
        raise FileExistsError(f"Se encontraron múltiples archivos que coinciden con '{nombre_parcial}':\n" + "\n".join(archivos_encontrados))

    ruta = archivos_encontrados[0]
    #print(f"📄 Cargando archivo: {ruta}")

    # Configura el parámetro `header`
    header_param = 0 if tiene_encabezado else None

    return pd.read_csv(ruta, encoding="utf-8", dtype=str, header=header_param)
