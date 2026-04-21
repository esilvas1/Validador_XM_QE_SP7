"""
URLs for validacion_app
"""
from django.urls import path
from . import views

app_name = 'validacion_app'

urlpatterns = [
    path('', views.index, name='index'),
    path('procesos/', views.procesos_view, name='procesos'),
    path('procesos/crear-consolidado-qe/', views.crear_consolidado_qe, name='crear_consolidado_qe'),
    path('procesos/crear-consolidado-qe-stream/', views.crear_consolidado_qe_stream, name='crear_consolidado_qe_stream'),
    path('procesos/crear-consolidado-sp7/', views.crear_consolidado_sp7, name='crear_consolidado_sp7'),
    path('procesos/crear-consolidado-sp7-stream/', views.crear_consolidado_sp7_stream, name='crear_consolidado_sp7_stream'),
    path('procesos/check-sp7-previo/', views.check_sp7_previo, name='check_sp7_previo'),
    path('procesos/crear-consolidado-xm/', views.crear_consolidado_xm, name='crear_consolidado_xm'),
    path('procesos/crear-consolidado-xm-stream/', views.crear_consolidado_xm_stream, name='crear_consolidado_xm_stream'),
    path('validacion/', views.validacion_view, name='validacion'),
    path('validacion/validar-sp7/', views.validar_sp7, name='validar_sp7'),
    path('validacion/validar-sp7-stream/', views.validar_sp7_stream, name='validar_sp7_stream'),
    path('validacion/validar-qe/', views.validar_qe, name='validar_qe'),
    path('validacion/validar-xm/', views.validar_xm, name='validar_xm'),
    path('validacion/descargar-csv/', views.descargar_csv_filtrado, name='descargar_csv_filtrado'),
    path('validacion/obtener-datos/', views.obtener_datos_filtrados, name='obtener_datos_filtrados'),
    path('archivos/', views.archivos_view, name='archivos'),
    path('archivos/listar/', views.listar_archivos, name='listar_archivos'),
    path('archivos/subir/', views.subir_archivo, name='subir_archivo'),
    path('archivos/descargar/', views.descargar_archivo, name='descargar_archivo'),
    path('archivos/eliminar/', views.eliminar_archivo, name='eliminar_archivo'),
    path('archivos/eliminar-todos/', views.eliminar_todos_archivos, name='eliminar_todos_archivos'),
    path('crear-qa-tfddregistro/', views.crear_qa_tfddregistro, name='crear_qa_tfddregistro'),
    path('crear-qa-tfddregistro-stream/', views.crear_qa_tfddregistro_stream, name='crear_qa_tfddregistro_stream'),
    path('test-conexion/', views.test_conexion, name='test_conexion'),
]
