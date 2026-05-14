"""
Autenticación de la sección Herramientas contra Oracle (tabla S0022_USUARIOS)
y contraseñas con bcrypt.
"""
import re
import secrets
import sys
from pathlib import Path

import bcrypt
from django.conf import settings
from sqlalchemy import text

_APP_DIR = Path(__file__).resolve().parent.parent
_SRC = _APP_DIR / 'src'
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from conexion import open_conexion, open_conexion_cim  # noqa: E402


def _safe_sql_identifier(name: str) -> str:
    if not name or not re.match(r'^[A-Za-z0-9_]+$', name):
        raise ValueError('Nombre de tabla/columna no válido (solo letras, números y _).')
    return name.upper()


def _tabla_calificada() -> str:
    """
    Nombre calificado para FROM:
    1) ORACLE_USUARIOS_ESQUEMA + nombre de tabla (si está definido),
    2) si no, ORACLE_CIM_OWNER + nombre de tabla (p. ej. CIM.S0022_USUARIOS),
    3) si no, ORACLE_USUARIOS_TABLA como OWNER.TABLA si trae un punto,
    4) si no, solo el nombre de tabla.
    """
    raw_tabla = (getattr(settings, 'ORACLE_USUARIOS_TABLA', None) or 'S0022_USUARIOS').strip() or 'S0022_USUARIOS'
    raw_schema = (getattr(settings, 'ORACLE_USUARIOS_ESQUEMA', '') or '').strip()
    cim_owner = (getattr(settings, 'ORACLE_CIM_OWNER', '') or '').strip()
    effective_schema = raw_schema or cim_owner

    if effective_schema:
        tabla_sola = raw_tabla.split('.')[-1]
        return f'{_safe_sql_identifier(effective_schema)}.{_safe_sql_identifier(tabla_sola)}'

    if '.' in raw_tabla:
        owner, nombre = raw_tabla.split('.', 1)
        return f'{_safe_sql_identifier(owner)}.{_safe_sql_identifier(nombre)}'

    return _safe_sql_identifier(raw_tabla)


def _formato_error_oracle(exc: Exception) -> str:
    msg = str(exc)
    if 'ORA-00942' in msg or '00942' in msg:
        msg += (
            ' Configure en .env el propietario: ORACLE_CIM_OWNER=CIM, o '
            'ORACLE_USUARIOS_ESQUEMA=..., o ORACLE_USUARIOS_TABLA=OWNER.S0022_USUARIOS. '
            'Compruebe ORACLE_CIM_* y permisos sobre la tabla.'
        )
    return msg


def _tabla_y_columnas():
    tabla = _tabla_calificada()
    col_u = _safe_sql_identifier(settings.ORACLE_USUARIOS_COL_USUARIO)
    col_p = _safe_sql_identifier(settings.ORACLE_USUARIOS_COL_PASSWORD)
    return tabla, col_u, col_p


def _open_usuarios_oracle():
    """
    Si ORACLE_CIM_* está completo, conecta solo a CIM (tabla de usuarios).
    Si no, usa la conexión principal ORACLE_* (comportamiento anterior).
    """
    if getattr(settings, 'ORACLE_CIM_CONFIGURED', False):
        conn, engine = open_conexion_cim()
        if conn is None:
            raise RuntimeError(
                'Variables ORACLE_CIM_* configuradas pero la conexión a Oracle CIM falló. '
                'Revise host, puerto, servicio, usuario y contraseña.'
            )
        return conn, engine
    conn, engine = open_conexion()
    if conn is None:
        raise RuntimeError('No se pudo conectar a Oracle. Revise ORACLE_* en .env.')
    return conn, engine


def obtener_hash_password(usuario: str) -> str | None:
    """Devuelve el hash bcrypt almacenado o None si no existe el usuario."""
    usuario = (usuario or '').strip()
    if not usuario:
        return None
    tabla, col_u, col_p = _tabla_y_columnas()
    sql = text(f'SELECT {col_p} FROM {tabla} WHERE {col_u} = :u')
    conn, engine = _open_usuarios_oracle()
    try:
        row = conn.execute(sql, {'u': usuario}).fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0]).strip()
    finally:
        conn.close()
        if engine is not None:
            engine.dispose()


def verificar_credenciales(usuario: str, password_plano: str) -> tuple[bool, str | None]:
    """
    Comprueba usuario y contraseña contra S0022_USUARIOS.
    Retorna (True, None) si OK, o (False, mensaje_error).
    """
    usuario = (usuario or '').strip()
    if not usuario or not password_plano:
        return False, 'Indique usuario y contraseña.'
    try:
        almacenado = obtener_hash_password(usuario)
    except Exception as e:
        return False, f'Error de conexión o consulta: {_formato_error_oracle(e)}'
    if not almacenado:
        return False, 'Usuario o contraseña incorrectos.'
    if _password_coincide_con_almacenado(almacenado, password_plano):
        return True, None
    return False, 'Usuario o contraseña incorrectos.'


def _password_coincide_con_almacenado(almacenado: str, password_plano: str) -> bool:
    """bcrypt si el valor parece hash; si no, comparación en texto (compat. legado)."""
    s = (almacenado or '').strip()
    if s.startswith(('$2a$', '$2b$', '$2y$', '$2x$')):
        try:
            return bcrypt.checkpw(password_plano.encode('utf-8'), s.encode('utf-8'))
        except ValueError:
            return False
    try:
        return secrets.compare_digest(s, password_plano)
    except TypeError:
        return False


def generar_hash_bcrypt(password_plano: str) -> str:
    return bcrypt.hashpw(password_plano.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def insertar_usuario(usuario: str, hash_bcrypt: str) -> None:
    tabla, col_u, col_p = _tabla_y_columnas()
    sql = text(f'INSERT INTO {tabla} ({col_u}, {col_p}) VALUES (:u, :h)')
    conn, engine = _open_usuarios_oracle()
    try:
        conn.execute(sql, {'u': usuario.strip(), 'h': hash_bcrypt})
        conn.commit()
    finally:
        conn.close()
        if engine is not None:
            engine.dispose()


def actualizar_password(usuario: str, hash_bcrypt: str) -> int:
    """Devuelve filas afectadas."""
    tabla, col_u, col_p = _tabla_y_columnas()
    sql = text(f'UPDATE {tabla} SET {col_p} = :h WHERE {col_u} = :u')
    conn, engine = _open_usuarios_oracle()
    try:
        result = conn.execute(sql, {'u': usuario.strip(), 'h': hash_bcrypt})
        conn.commit()
        return int(result.rowcount or 0)
    finally:
        conn.close()
        if engine is not None:
            engine.dispose()
