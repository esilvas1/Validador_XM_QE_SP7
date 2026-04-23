Siguiente paso recomendado

1) Montar el DFS en el host Linux
- Crear punto de montaje: /mnt/dfs
- Montar recurso CIFS/SMB del DFS con usuario de dominio autorizado

2) Publicar el mount al contenedor
- En docker-compose.prod.yml agregar volumen host->contenedor, por ejemplo:
	/mnt/dfs/S0022/data:/mnt/dfs/S0022/data

3) Definir ruta Linux dentro del contenedor
- Conservar DATA_DIR para escritorio Windows.
- En servidor Linux definir DATA_DIR_LINUX=/mnt/dfs/S0022/data
- La app ahora usa DATA_DIR_LINUX como override en Linux.

4) Redeploy
- docker compose -f docker-compose.prod.yml up -d --force-recreate

5) Verificar ruta real y estado
- En la pantalla Gestion de Archivos se muestra:
	- DATA_DIR configurada
	- Ruta real en el servidor
	- Estado de acceso
	- Carpetas destino

6) Verificar desde terminal del contenedor
- docker exec -it validador-xm-qe sh -lc 'echo "$DATA_DIR_LINUX" && ls -la /mnt/dfs/S0022/data'