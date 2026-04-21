# Validador OMS SDL - Aplicación Web Django

## Descripción
Sistema web de validación de ajuste mensual para el OMS del SDL. Esta aplicación ha sido convertida de una aplicación de escritorio (tkinter) a una aplicación web moderna usando Django.

## Características
- ✅ Interfaz web moderna y responsive
- ✅ Prueba de conexión a Oracle
- ✅ Creación de consolidados (QE, SP7, XM)
- ✅ Validación de datos
- ✅ Generación de QA_TFDDREGISTRO
- ✅ Consola web en tiempo real
- ✅ Toda la lógica de negocio se mantiene intacta

## Requisitos Previos
- Python 3.8 o superior
- pip (gestor de paquetes de Python)
- Acceso a base de datos Oracle (configurado en src/conexion.py)

## Instalación

### 1. Instalar dependencias
```powershell
cd ValidacionAjusteMensual
pip install -r requirements.txt
```

### 2. Configurar Django
```powershell
# Crear las migraciones de base de datos
python manage.py migrate

# (Opcional) Crear un superusuario para acceder al admin de Django
python manage.py createsuperuser
```

### 3. Ejecutar el servidor de desarrollo
```powershell
python manage.py runserver
```

### 4. Acceder a la aplicación
Abrir el navegador web en:
```
http://localhost:8000
```

## Estructura del Proyecto

```
ValidacionAjusteMensual/
├── manage.py                 # Script principal de Django
├── requirements.txt          # Dependencias del proyecto
├── config/                   # Configuración de Django
│   ├── settings.py          # Configuración principal
│   ├── urls.py              # URLs principales
│   └── wsgi.py              # Configuración WSGI
├── validacion_app/          # Aplicación Django principal
│   ├── views.py             # Vistas web (wrappers de la lógica)
│   ├── urls.py              # URLs de la aplicación
│   └── ...
├── templates/               # Plantillas HTML
│   ├── base.html           # Plantilla base
│   └── validacion_app/     # Templates específicos
│       ├── index.html
│       ├── procesos.html
│       └── validacion.html
├── src/                     # Lógica de negocio (SIN MODIFICAR)
│   ├── processor.py         # Procesamiento de datos
│   ├── validator.py         # Validaciones
│   ├── loader.py            # Carga de archivos
│   ├── conexion.py          # Conexión a Oracle
│   ├── web_utils.py         # Utilidades web
│   └── ...
└── data/                    # Datos y archivos CSV
```

## Uso

### Página Principal
La página principal muestra 4 opciones:

1. **🔌 Probar Conexión Oracle**: Verifica la conectividad con la base de datos
2. **▶ Ejecutar Extracción**: Crea los consolidados QE, SP7 y XM
3. **✓ Validación de Datos**: Valida los consolidados existentes
4. **📊 Generación QA_TFDDREGISTRO**: Genera el archivo QA_TFDDREGISTRO

### Proceso de Extracción
1. Click en "Ejecutar Extracción"
2. Seleccionar el consolidado a crear
3. Confirmar la operación
4. Ver el progreso en la consola web

### Validación de Datos
1. Click en "Validación de Datos"
2. Seleccionar el consolidado a validar
3. Confirmar la operación
4. Ver resultados en la consola web

## Diferencias con la Versión Desktop

### ✅ Mantenido
- Toda la lógica de negocio en `src/`
- Procesamiento de datos
- Validaciones
- Conexión a Oracle
- Generación de reportes

### 🔄 Cambiado
- **Interfaz**: De tkinter (ventanas) a Django (web)
- **Confirmaciones**: De messagebox a diálogos JavaScript
- **Consola**: De widget Text a consola web
- **Ejecución**: De aplicación desktop a servidor web

### ❌ Removido
- Dependencia de tkinter
- Splash screen de inicio
- Ventanas modales

## Notas Importantes

1. **Archivos de Datos**: Los archivos CSV deben estar en la carpeta `data/`
2. **Conexión Oracle**: Configurar credenciales en `src/conexion.py`
3. **Producción**: Para producción, cambiar `DEBUG = False` en `config/settings.py` y configurar `SECRET_KEY`
4. **Procesamiento Largo**: Operaciones largas pueden causar timeout. Considerar usar Celery para producción.

## Solución de Problemas

### Error de importación de módulos
Asegúrese de que todas las dependencias estén instaladas:
```powershell
pip install -r requirements.txt
```

### Error de conexión a Oracle
Verifique las credenciales en `src/conexion.py`

### Puerto ya en uso
Si el puerto 8000 está ocupado, use otro:
```powershell
python manage.py runserver 8080
```

## Desarrollo Futuro

Posibles mejoras:
- [ ] Sistema de autenticación de usuarios
- [ ] Procesamiento asíncrono con Celery
- [ ] API REST para integración
- [ ] Dashboard con gráficos
- [ ] Exportación de reportes en diferentes formatos
- [ ] Sistema de notificaciones

## Créditos
**Desarrollado por**: Edwin Silva  
**Organización**: Centrales Eléctricas de N.S.  
**Año**: 2026

---

Para volver a usar la versión desktop (tkinter):
```powershell
cd src
python main.py
```
