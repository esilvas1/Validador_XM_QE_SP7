"""
Views for validacion_app - Mantiene la lógica original de src/
"""
from django.shortcuts import render, redirect
from django.http import JsonResponse, StreamingHttpResponse
from django.contrib import messages
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
import sys
import os
import io
import traceback
import queue
import threading
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr

# Agregar el directorio src al path para importar los módulos originales
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(BASE_DIR, 'src')
sys.path.insert(0, SRC_DIR)

FOLDERS_MAP = {
    'qe': 'Descargados QE',
    'xm': 'Descargados XM',
    'rm': 'Reporte Mensual',
    'resultados': 'Resultados',
}


def _resolver_data_dir_para_dfs():
    """Valida que DATA_DIR apunte a un DFS real accesible en el servidor actual."""
    data_dir = Path(settings.DATA_DIR)
    configurada = str(data_dir)
    real = os.path.realpath(configurada)

    if os.name != 'nt' and configurada.startswith('\\\\'):
        return {
            'data_dir': data_dir,
            'data_dir_configurada': configurada,
            'data_dir_real': real,
            'accesible': False,
            'error': (
                "DATA_DIR usa ruta UNC de Windows en Linux. "
                "Monte el DFS y configure DATA_DIR con ruta POSIX "
                "(ejemplo: /mnt/dfs/S0022/data)."
            ),
        }

    accesible = data_dir.exists() and data_dir.is_dir()
    return {
        'data_dir': data_dir,
        'data_dir_configurada': configurada,
        'data_dir_real': real,
        'accesible': accesible,
        'error': None if accesible else f"DATA_DIR no existe o no es accesible: {configurada}",
    }


def index(request):
    """Página principal"""
    return render(request, 'validacion_app/index.html')


def procesos_view(request):
    """Vista para iniciar el proceso de extracción"""
    return render(request, 'validacion_app/procesos.html')


def validacion_view(request):
    """Vista para iniciar la validación"""
    return render(request, 'validacion_app/validacion.html')


def test_conexion(request):
    """Prueba la conexión a Oracle"""
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            print("Iniciando conexión a Oracle...")
            from conexion import open_conexion
            conn, engine = open_conexion()
            
            if conn:
                print("[OK] Conexión lista para consultas.")
                conn.close()
                engine.dispose()
                print("[INFO] Conexión cerrada correctamente.")
                success = True
            else:
                success = False
    except Exception as e:
        print(f"[ERROR] Error: {str(e)}")
        success = False
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'output': output,
        'errors': errors
    })


def crear_consolidado_qe(request):
    """Crea el consolidado QE"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from processor import crear_CONSOLIDADO_QE
            crear_CONSOLIDADO_QE()
            success = True
            message = "CONSOLIDADO_QE creado correctamente"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
        error_buffer.write(f"\n{'='*60}\n")
        error_buffer.write(f"ERROR DETALLADO:\n")
        error_buffer.write(f"{'='*60}\n")
        error_buffer.write(traceback.format_exc())
        error_buffer.write(f"\n{'='*60}\n")
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors
    })


@csrf_exempt
def crear_consolidado_qe_stream(request):
    """Streaming SSE del proceso crear CONSOLIDADO_QE — muestra output en tiempo real"""

    output_queue = queue.Queue()

    class StreamWriter:
        def __init__(self, prefix=''):
            self.prefix = prefix

        def write(self, text):
            if text and text.strip():
                output_queue.put(('output', text))

        def flush(self):
            pass

    def run_process():
        try:
            with redirect_stdout(StreamWriter()), redirect_stderr(StreamWriter('ERR: ')):
                import importlib
                import processor as proc_module
                importlib.reload(proc_module)
                proc_module.crear_CONSOLIDADO_QE()
            output_queue.put(('done', 'success'))
        except Exception as e:
            output_queue.put(('output', f'\n{"="*60}\nERROR DETALLADO:\n{"="*60}\n{traceback.format_exc()}'))
            output_queue.put(('done', 'error:' + str(e)))

    thread = threading.Thread(target=run_process, daemon=True)
    thread.start()

    def event_generator():
        yield "retry: 2000\n\n"
        while True:
            try:
                event_type, data = output_queue.get(timeout=600)
                if event_type == 'output':
                    for line in data.splitlines():
                        escaped = line.replace('\\', '\\\\').replace('\n', ' ')
                        yield f"data: {escaped}\n"
                    yield "\n"
                elif event_type == 'done':
                    yield f"event: done\ndata: {data}\n\n"
                    break
            except queue.Empty:
                yield ": keepalive\n\n"

    response = StreamingHttpResponse(event_generator(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


def check_sp7_previo(request):
    """Verifica si existe un archivo de descarga previa de SP7 solo en DATA_DIR/Reporte Mensual."""
    import glob
    import zipfile as _zipfile
    from datetime import datetime as _dt
    data_dir = Path(settings.DATA_DIR)
    # Buscar por prefijo base, ignorando cualquier sufijo de versión/período
    nombre_base = "Consulta_Eventos_Cens_Apertura_Cierre"

    if not data_dir:
        return JsonResponse({'found': False, 'message': 'DATA_DIR no configurado. Debe apuntar al DFS.'})

    carpeta = os.path.join(str(data_dir), 'Reporte Mensual')
    if not os.path.isdir(carpeta):
        return JsonResponse({'found': False, 'message': f'No se puede acceder al DFS en: {carpeta}'})

    # Buscar CSV directo
    candidatos = sorted(
        glob.glob(os.path.join(carpeta, f"*{nombre_base}*.csv")),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )
    candidatos_finales = [p for p in candidatos if "_parcial" not in os.path.basename(p).lower()]
    if candidatos_finales:
        candidatos = candidatos_finales
    if candidatos:
        ruta = candidatos[0]
        stat = os.stat(ruta)
        return JsonResponse({
            'found': True,
            'filename': os.path.basename(ruta),
            'fecha_mod': _dt.fromtimestamp(stat.st_mtime).strftime('%d/%m/%Y %H:%M'),
            'size_kb': round(stat.st_size / 1024, 1),
            'carpeta': carpeta,
            'periodo': 'Detectado en DFS'
        })

    # Buscar dentro de ZIPs
    zip_candidates = sorted(
        glob.glob(os.path.join(carpeta, f"*{nombre_base}*.zip")),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )
    for zip_path in zip_candidates:
        try:
            stat = os.stat(zip_path)
            with _zipfile.ZipFile(zip_path, 'r') as zf:
                if any(n.lower().endswith('.csv') for n in zf.namelist()):
                    return JsonResponse({
                        'found': True,
                        'filename': os.path.basename(zip_path),
                        'fecha_mod': _dt.fromtimestamp(stat.st_mtime).strftime('%d/%m/%Y %H:%M'),
                        'size_kb': round(stat.st_size / 1024, 1),
                        'carpeta': carpeta,
                        'periodo': 'Detectado en DFS'
                    })
        except Exception:
            continue

    return JsonResponse({'found': False, 'message': f'No se encontró {nombre_base} en DATA_DIR/Reporte Mensual'})


def crear_consolidado_sp7(request):
    """Crea el consolidado SP7"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from processor import crear_CONSOLIDADO_SP7 
            crear_CONSOLIDADO_SP7()
            success = True
            message = "CONSOLIDADO_SP7 creado correctamente"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
        error_buffer.write(f"\n{'='*60}\n")
        error_buffer.write(f"ERROR DETALLADO:\n")
        error_buffer.write(f"{'='*60}\n")
        error_buffer.write(traceback.format_exc())
        error_buffer.write(f"\n{'='*60}\n")
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors
    })


@csrf_exempt
def crear_consolidado_sp7_stream(request):
    """Streaming SSE del proceso crear CONSOLIDADO_SP7 — muestra output en tiempo real"""

    forzar_descarga = request.GET.get('forzar_descarga', 'false').lower() == 'true'

    output_queue = queue.Queue()

    class StreamWriter:
        def __init__(self, prefix=''):
            self.prefix = prefix

        def write(self, text):
            if text and text.strip():
                output_queue.put(('output', text))

        def flush(self):
            pass

    def run_process():
        try:
            with redirect_stdout(StreamWriter()), redirect_stderr(StreamWriter('ERR: ')):
                import importlib
                import processor as proc_module
                importlib.reload(proc_module)
                proc_module.crear_CONSOLIDADO_SP7(forzar_descarga=forzar_descarga)
            output_queue.put(('done', 'success'))
        except Exception as e:
            output_queue.put(('output', f'\n{"="*60}\nERROR DETALLADO:\n{"="*60}\n{traceback.format_exc()}'))
            output_queue.put(('done', 'error:' + str(e)))

    thread = threading.Thread(target=run_process, daemon=True)
    thread.start()

    def event_generator():
        yield "retry: 2000\n\n"
        while True:
            try:
                event_type, data = output_queue.get(timeout=600)
                if event_type == 'output':
                    for line in data.splitlines():
                        escaped = line.replace('\\', '\\\\').replace('\n', ' ')
                        yield f"data: {escaped}\n"
                    yield "\n"
                elif event_type == 'done':
                    yield f"event: done\ndata: {data}\n\n"
                    break
            except queue.Empty:
                yield ": keepalive\n\n"

    response = StreamingHttpResponse(event_generator(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


def crear_consolidado_xm(request):
    """Crea el consolidado XM"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from processor import crear_CONSOLIDADO_XM
            crear_CONSOLIDADO_XM()
            success = True
            message = "CONSOLIDADO_XM creado correctamente"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
        error_buffer.write(f"\n{'='*60}\n")
        error_buffer.write(f"ERROR DETALLADO:\n")
        error_buffer.write(f"{'='*60}\n")
        error_buffer.write(traceback.format_exc())
        error_buffer.write(f"\n{'='*60}\n")
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors
    })


@csrf_exempt
def crear_consolidado_xm_stream(request):
    """Streaming SSE del proceso crear CONSOLIDADO_XM — muestra output en tiempo real"""

    output_queue = queue.Queue()

    class StreamWriter:
        def __init__(self, prefix=''):
            self.prefix = prefix

        def write(self, text):
            if text and text.strip():
                output_queue.put(('output', text))

        def flush(self):
            pass

    def run_process():
        try:
            with redirect_stdout(StreamWriter()), redirect_stderr(StreamWriter('ERR: ')):
                import importlib
                import processor as proc_module
                importlib.reload(proc_module)
                proc_module.crear_CONSOLIDADO_XM()
            output_queue.put(('done', 'success'))
        except Exception as e:
            output_queue.put(('output', f'\n{"="*60}\nERROR DETALLADO:\n{"="*60}\n{traceback.format_exc()}'))
            output_queue.put(('done', 'error:' + str(e)))

    thread = threading.Thread(target=run_process, daemon=True)
    thread.start()

    def event_generator():
        yield "retry: 2000\n\n"
        while True:
            try:
                event_type, data = output_queue.get(timeout=600)
                if event_type == 'output':
                    for line in data.splitlines():
                        escaped = line.replace('\\', '\\\\').replace('\n', ' ')
                        yield f"data: {escaped}\n"
                    yield "\n"
                elif event_type == 'done':
                    yield f"event: done\ndata: {data}\n\n"
                    break
            except queue.Empty:
                yield ": keepalive\n\n"

    response = StreamingHttpResponse(event_generator(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


def validar_sp7_stream(request):
    """Streaming SSE de validación SP7 — muestra output en tiempo real"""

    output_queue = queue.Queue()

    class StreamWriter:
        def __init__(self, prefix=''):
            self.prefix = prefix

        def write(self, text):
            if text and text.strip():
                output_queue.put(('output', self.prefix + text))

        def flush(self):
            pass

    def run_process():
        try:
            with redirect_stdout(StreamWriter()), redirect_stderr(StreamWriter('ERR: ')):
                import importlib
                import validator as val_module
                importlib.reload(val_module)
                val_module.validar_CONSOLIDADO_SP7()

            # Generar dashboard tras el proceso exitoso
            import json
            import pandas as pd
            from pathlib import Path
            csv_path = Path(settings.DATA_DIR) / 'Resultados' / 'CONSOLIDADO_SP7.csv'
            if not csv_path.exists():
                csv_path = Path(settings.DATA_DIR) / 'CONSOLIDADO_SP7.csv'
            dashboard_data = None
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                if 'ESTADO_VALIDACION' in df.columns:
                    estado_counts = df['ESTADO_VALIDACION'].value_counts().to_dict()
                    total = len(df)
                    dashboard_data = {
                        'total_registros': total,
                        'estado_counts': estado_counts,
                        'estado_percentages': {
                            e: round((c / total) * 100, 2) for e, c in estado_counts.items()
                        },
                        'archivo': 'CONSOLIDADO_SP7.csv'
                    }
            payload = json.dumps({'status': 'success', 'dashboard': dashboard_data})
            output_queue.put(('done', payload))
        except Exception as e:
            output_queue.put(('output', f'\n{"="*60}\nERROR DETALLADO:\n{"="*60}\n{traceback.format_exc()}'))
            output_queue.put(('done', f'error:{str(e)}'))

    thread = threading.Thread(target=run_process, daemon=True)
    thread.start()

    def event_generator():
        yield "retry: 2000\n\n"
        while True:
            try:
                event_type, data = output_queue.get(timeout=600)
                if event_type == 'output':
                    for line in data.splitlines():
                        escaped = line.replace('\\', '\\\\').replace('\n', ' ')
                        yield f"data: {escaped}\n"
                    yield "\n"
                elif event_type == 'done':
                    yield f"event: done\ndata: {data}\n\n"
                    break
            except queue.Empty:
                yield ": keepalive\n\n"

    response = StreamingHttpResponse(event_generator(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


def validar_sp7(request):
    """Valida el consolidado SP7"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from validator import validar_CONSOLIDADO_SP7
            validar_CONSOLIDADO_SP7()
            success = True
            message = "Validación SP7 completada"
        
        # Generar estadísticas del dashboard
        import pandas as pd
        from pathlib import Path
        
        # Buscar primero en data/Resultados, luego en DATA_DIR
        csv_path_resultados = Path(settings.DATA_DIR) / 'Resultados' / 'CONSOLIDADO_SP7.csv'
        csv_path_base = Path(settings.DATA_DIR) / 'CONSOLIDADO_SP7.csv'
        csv_path = csv_path_resultados if csv_path_resultados.exists() else csv_path_base
        dashboard_data = None
        
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            
            if 'ESTADO_VALIDACION' in df.columns:
                # Contar estados de validación
                estado_counts = df['ESTADO_VALIDACION'].value_counts().to_dict()
                total_registros = len(df)
                
                # Calcular porcentajes
                estado_percentages = {
                    estado: round((count / total_registros) * 100, 2)
                    for estado, count in estado_counts.items()
                }
                
                dashboard_data = {
                    'total_registros': total_registros,
                    'estado_counts': estado_counts,
                    'estado_percentages': estado_percentages,
                    'archivo': 'CONSOLIDADO_SP7.csv'
                }
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
        dashboard_data = None
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors,
        'dashboard': dashboard_data
    })


def validar_qe(request):
    """Valida el consolidado QE"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from validator import validar_CONSOLIDADO_QE
            validar_CONSOLIDADO_QE()
            success = True
            message = "Validación QE completada"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors
    })


def validar_xm(request):
    """Valida el consolidado XM"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from validator import validar_CONSOLIDADO_XM
            validar_CONSOLIDADO_XM()
            success = True
            message = "Validación XM completada"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors
    })


def crear_qa_tfddregistro(request):
    """Crea el QA_TFDDREGISTRO"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)
    
    output_buffer = io.StringIO()
    error_buffer = io.StringIO()
    
    try:
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            from crear_data_qa_tfddregistro import crear_QA_TFDDREGISTRO
            crear_QA_TFDDREGISTRO()
            success = True
            message = "QA_TFDDREGISTRO creado correctamente"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
    
    output = output_buffer.getvalue()
    errors = error_buffer.getvalue()
    
    return JsonResponse({
        'success': success,
        'message': message,
        'output': output,
        'errors': errors
    })


@csrf_exempt
def crear_qa_tfddregistro_stream(request):
    """Streaming SSE del proceso crear QA_TFDDREGISTRO — muestra output en tiempo real"""

    output_queue = queue.Queue()

    class StreamWriter:
        def __init__(self, prefix=''):
            self.prefix = prefix

        def write(self, text):
            if text and text.strip():
                output_queue.put(('output', text))

        def flush(self):
            pass

    def run_process():
        try:
            with redirect_stdout(StreamWriter()), redirect_stderr(StreamWriter('ERR: ')):
                import importlib
                import crear_data_qa_tfddregistro as qa_module
                importlib.reload(qa_module)
                qa_module.crear_QA_TFDDREGISTRO()
            output_queue.put(('done', 'success'))
        except Exception as e:
            output_queue.put(('output', f'\n{"="*60}\nERROR DETALLADO:\n{"="*60}\n{traceback.format_exc()}'))
            output_queue.put(('done', 'error:' + str(e)))

    thread = threading.Thread(target=run_process, daemon=True)
    thread.start()

    def event_generator():
        yield "retry: 2000\n\n"
        while True:
            try:
                event_type, data = output_queue.get(timeout=600)
                if event_type == 'output':
                    for line in data.splitlines():
                        escaped = line.replace('\\', '\\\\').replace('\n', ' ')
                        yield f"data: {escaped}\n"
                    yield "\n"
                elif event_type == 'done':
                    yield f"event: done\ndata: {data}\n\n"
                    break
            except queue.Empty:
                yield ": keepalive\n\n"

    response = StreamingHttpResponse(event_generator(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


def descargar_csv_filtrado(request):
    """Descarga CSV filtrado por estado de validación"""
    from django.http import HttpResponse
    import pandas as pd
    from pathlib import Path
    
    estado = request.GET.get('estado', '')
    archivo = request.GET.get('archivo', 'CONSOLIDADO_SP7.csv')
    
    # Buscar primero en data/Resultados, luego en DATA_DIR
    csv_path_resultados = Path(settings.DATA_DIR) / 'Resultados' / archivo
    csv_path_base = Path(settings.DATA_DIR) / archivo
    csv_path = csv_path_resultados if csv_path_resultados.exists() else csv_path_base
    
    if not csv_path.exists():
        return HttpResponse(f'Archivo {archivo} no encontrado', status=404)
    
    try:
        # Leer CSV
        df = pd.read_csv(csv_path)
        
        # Filtrar por estado si no es "TODOS"
        if estado and estado != 'TODOS':
            if 'ESTADO_VALIDACION' in df.columns:
                df_filtrado = df[df['ESTADO_VALIDACION'] == estado]
            else:
                return HttpResponse('Columna ESTADO_VALIDACION no encontrada', status=400)
        else:
            df_filtrado = df
        
        # Crear respuesta HTTP con CSV
        response = HttpResponse(content_type='text/csv; charset=utf-8-sig')
        
        # Nombre del archivo descargable
        nombre_archivo = f"{archivo.replace('.csv', '')}_{estado.replace(' ', '_')}.csv"
        response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'
        
        # Escribir CSV a la respuesta
        df_filtrado.to_csv(response, index=False, encoding='utf-8-sig')
        
        return response
        
    except Exception as e:
        return HttpResponse(f'Error al procesar archivo: {str(e)}', status=500)


def obtener_datos_filtrados(request):
    """Obtiene datos filtrados en formato JSON para mostrar en tabla"""
    import pandas as pd
    from pathlib import Path
    
    estado = request.GET.get('estado', '')
    archivo = request.GET.get('archivo', 'CONSOLIDADO_SP7.csv')
    
    # Buscar primero en data/Resultados, luego en DATA_DIR
    csv_path_resultados = Path(settings.DATA_DIR) / 'Resultados' / archivo
    csv_path_base = Path(settings.DATA_DIR) / archivo
    csv_path = csv_path_resultados if csv_path_resultados.exists() else csv_path_base
    
    if not csv_path.exists():
        return JsonResponse({'success': False, 'message': f'Archivo {archivo} no encontrado'}, status=404)
    
    try:
        # Leer CSV
        df = pd.read_csv(csv_path)
        
        # Filtrar por estado si no es "TODOS"
        if estado and estado != 'TODOS':
            if 'ESTADO_VALIDACION' in df.columns:
                df_filtrado = df[df['ESTADO_VALIDACION'] == estado]
            else:
                return JsonResponse({'success': False, 'message': 'Columna ESTADO_VALIDACION no encontrada'}, status=400)
        else:
            df_filtrado = df
        
        # Usar TODAS las columnas del DataFrame
        columnas_disponibles = df_filtrado.columns.tolist()
        
        # Limitar a primeras 1000 filas para no saturar el navegador
        df_limitado = df_filtrado[columnas_disponibles].head(1000)
        
        # Reemplazar NaN y valores nulos antes de convertir a JSON
        df_limitado = df_limitado.fillna('')
        
        # Convertir a formato JSON
        registros = df_limitado.to_dict('records')
        
        return JsonResponse({
            'success': True,
            'registros': registros,
            'columnas': columnas_disponibles,
            'total': len(df_filtrado),
            'mostrando': len(registros)
        })
        
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Error al procesar archivo: {str(e)}'}, status=500)


def archivos_view(request):
    """Vista para gestión de archivos"""
    dfs_info = _resolver_data_dir_para_dfs()
    data_dir = dfs_info['data_dir']
    return render(
        request,
        'validacion_app/archivos.html',
        {
            'data_dir_configurada': dfs_info['data_dir_configurada'],
            'data_dir_real': dfs_info['data_dir_real'],
            'data_dir_accesible': dfs_info['accesible'],
            'data_dir_error': dfs_info['error'],
            'ruta_qe': str(data_dir / FOLDERS_MAP['qe']),
            'ruta_xm': str(data_dir / FOLDERS_MAP['xm']),
            'ruta_rm': str(data_dir / FOLDERS_MAP['rm']),
            'ruta_resultados': str(data_dir / FOLDERS_MAP['resultados']),
        },
    )


def listar_archivos(request):
    """Lista archivos de una carpeta específica"""

    carpeta = request.GET.get('carpeta', '')

    if carpeta not in FOLDERS_MAP:
        return JsonResponse({'success': False, 'message': 'Carpeta no válida'}, status=400)

    dfs_info = _resolver_data_dir_para_dfs()
    if dfs_info['error']:
        return JsonResponse(
            {
                'success': False,
                'message': dfs_info['error'],
                'data_dir': dfs_info['data_dir_configurada'],
                'data_dir_real': dfs_info['data_dir_real'],
            },
            status=503,
        )

    carpeta_path = dfs_info['data_dir'] / FOLDERS_MAP[carpeta]
    
    # Crear carpeta si no existe
    if not carpeta_path.exists():
        carpeta_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # Listar archivos (sin subdirectorios)
        archivos = []
        for item in carpeta_path.iterdir():
            if item.is_file():
                archivos.append(item.name)
        
        archivos.sort()  # Ordenar alfabéticamente
        
        return JsonResponse({
            'success': True,
            'archivos': archivos,
            'total': len(archivos),
            'ruta_carpeta': str(carpeta_path),
            'data_dir': dfs_info['data_dir_configurada'],
            'data_dir_real': dfs_info['data_dir_real'],
        })
        
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Error al listar archivos: {str(e)}'}, status=500)


def subir_archivo(request):
    """Sube uno o más archivos a una carpeta específica"""

    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)

    carpeta = request.POST.get('carpeta', '')
    archivos = request.FILES.getlist('archivos')

    if carpeta not in FOLDERS_MAP:
        return JsonResponse({'success': False, 'message': 'Carpeta no válida'}, status=400)

    if not archivos:
        return JsonResponse({'success': False, 'message': 'No se recibieron archivos'}, status=400)

    dfs_info = _resolver_data_dir_para_dfs()
    if dfs_info['error']:
        return JsonResponse(
            {
                'success': False,
                'message': dfs_info['error'],
                'data_dir': dfs_info['data_dir_configurada'],
                'data_dir_real': dfs_info['data_dir_real'],
            },
            status=503,
        )

    carpeta_path = dfs_info['data_dir'] / FOLDERS_MAP[carpeta]
    carpeta_path.mkdir(parents=True, exist_ok=True)

    try:
        subidos = 0
        for archivo in archivos:
            file_path = carpeta_path / archivo.name

            # Guardar archivo
            with open(file_path, 'wb+') as destination:
                for chunk in archivo.chunks():
                    destination.write(chunk)

            subidos += 1

        return JsonResponse({
            'success': True,
            'message': f'{subidos} archivo(s) subido(s)',
            'subidos': subidos,
            'ruta_carpeta': str(carpeta_path),
            'data_dir': dfs_info['data_dir_configurada'],
            'data_dir_real': dfs_info['data_dir_real'],
        })
        
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Error al subir archivos: {str(e)}'}, status=500)


def descargar_archivo(request):
    """Descarga un archivo de una carpeta específica"""
    from django.http import FileResponse, Http404

    carpeta = request.GET.get('carpeta', '')
    archivo = request.GET.get('archivo', '')

    if carpeta not in FOLDERS_MAP or not archivo:
        raise Http404('Archivo no encontrado')

    dfs_info = _resolver_data_dir_para_dfs()
    if dfs_info['error']:
        raise Http404(dfs_info['error'])

    file_path = dfs_info['data_dir'] / FOLDERS_MAP[carpeta] / archivo
    
    if not file_path.exists() or not file_path.is_file():
        raise Http404('Archivo no encontrado')
    
    try:
        response = FileResponse(open(file_path, 'rb'))
        response['Content-Disposition'] = f'attachment; filename="{archivo}"'
        return response
        
    except Exception as e:
        raise Http404(f'Error al descargar archivo: {str(e)}')


def eliminar_archivo(request):
    """Elimina un archivo de una carpeta específica"""
    import json

    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)

    try:
        data = json.loads(request.body)
        carpeta = data.get('carpeta', '')
        archivo = data.get('archivo', '')
    except:
        return JsonResponse({'success': False, 'message': 'Datos inválidos'}, status=400)
    
    if carpeta not in FOLDERS_MAP or not archivo:
        return JsonResponse({'success': False, 'message': 'Parámetros inválidos'}, status=400)

    dfs_info = _resolver_data_dir_para_dfs()
    if dfs_info['error']:
        return JsonResponse(
            {
                'success': False,
                'message': dfs_info['error'],
                'data_dir': dfs_info['data_dir_configurada'],
                'data_dir_real': dfs_info['data_dir_real'],
            },
            status=503,
        )

    file_path = dfs_info['data_dir'] / FOLDERS_MAP[carpeta] / archivo
    
    if not file_path.exists() or not file_path.is_file():
        return JsonResponse({'success': False, 'message': 'Archivo no encontrado'}, status=404)
    
    try:
        os.remove(file_path)
        return JsonResponse({
            'success': True,
            'message': 'Archivo eliminado correctamente'
        })
        
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Error al eliminar archivo: {str(e)}'}, status=500)


def eliminar_todos_archivos(request):
    """Elimina todos los archivos de una carpeta específica"""
    import json

    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Método no permitido'}, status=405)

    try:
        data = json.loads(request.body)
        carpeta = data.get('carpeta', '')
    except:
        return JsonResponse({'success': False, 'message': 'Datos inválidos'}, status=400)

    if carpeta not in FOLDERS_MAP:
        return JsonResponse({'success': False, 'message': 'Carpeta no válida'}, status=400)

    dfs_info = _resolver_data_dir_para_dfs()
    if dfs_info['error']:
        return JsonResponse(
            {
                'success': False,
                'message': dfs_info['error'],
                'data_dir': dfs_info['data_dir_configurada'],
                'data_dir_real': dfs_info['data_dir_real'],
            },
            status=503,
        )

    carpeta_path = dfs_info['data_dir'] / FOLDERS_MAP[carpeta]
    
    if not carpeta_path.exists():
        return JsonResponse({'success': False, 'message': 'Carpeta no encontrada'}, status=404)
    
    try:
        eliminados = 0
        errores = []
        for item in carpeta_path.iterdir():
            if item.is_file():
                try:
                    os.remove(item)
                    eliminados += 1
                except Exception as e:
                    errores.append(f'{item.name}: {str(e)}')
        
        if errores:
            return JsonResponse({
                'success': True,
                'message': f'{eliminados} archivo(s) eliminado(s). Errores: {", ".join(errores)}',
                'eliminados': eliminados
            })
        
        return JsonResponse({
            'success': True,
            'message': f'{eliminados} archivo(s) eliminado(s) correctamente',
            'eliminados': eliminados
        })
        
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Error al eliminar archivos: {str(e)}'}, status=500)
