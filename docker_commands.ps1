docker compose up -d --build #construir y levantar el contenedor
docker save -o validacion.tar validacionajustemensual-app #guardar la imagen del contenedor en un archivo tar

opcional:
docker build --no-cache -t solucion_s0065_app:latest .