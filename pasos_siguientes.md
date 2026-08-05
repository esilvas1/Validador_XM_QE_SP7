# Despliegue en Linux: DFS, DNS e IP

## Qué hace cada variable (importante)

| Variable | Dónde aplica | Qué debe contener |
|----------|--------------|-------------------|
| `DATA_DIR` | Windows (PyCharm, `runserver`) | Ruta UNC: `\\servidor\share\...\S0022\data` |
| `DATA_DIR_LINUX` | Linux / contenedor Docker | Ruta **ya montada** en disco: `/mnt/dfs/.../S0022/data` |

La aplicación Django **no abre** el share por nombre de dominio en Linux. Solo lee archivos en `DATA_DIR_LINUX`.

El problema de DNS afecta al **montaje del sistema operativo** (antes de Docker), no al valor de `DATA_DIR_LINUX` en sí.

```
[Servidor archivo]  ←── aquí va IP o /etc/hosts (CIFS mount)
        ↓
/mnt/dfs/CISO01_CIMDESS/...   ←── DATA_DIR_LINUX apunta aquí
        ↓
[volumen Docker] → contenedor validador-xm-qe
```

---

## 1) Montar el DFS en el host Linux (donde suele ir la IP)

**IP del servidor de archivos:** `10.42.10.61` (equivalente a `CENS-AD02.cens.corp.epm.com.co` cuando DNS falla).

### Opción A — Montaje con IP (recomendada si DNS falla)

```bash
sudo mkdir -p /mnt/dfs/CISO01_CIMDESS

# Ejemplo: ajuste //IP/share y credenciales con su equipo de infra
sudo mount -t cifs //10.42.10.61/Administrativa/UO01/CISO01/CISO01_CIMDESS \
  /mnt/dfs/CISO01_CIMDESS \
  -o username=USUARIO_DOMINIO,password='CLAVE',domain=CENS,corp.epm.com.co,uid=1000,gid=1000,file_mode=0660,dir_mode=0770
```

Compruebe:

```bash
ls -la /mnt/dfs/CISO01_CIMDESS/S0022/data
```

### Opción B — Seguir con nombre DNS en fstab pero fijar IP en `/etc/hosts`

Si prefieren mantener el nombre en scripts:

```bash
# /etc/hosts (ejemplo)
10.42.10.61   CENS-AD02.cens.corp.epm.com.co
```

Luego el `mount` puede seguir usando el FQDN; el SO resolverá por IP.

### Montaje permanente (`/etc/fstab`)

```fstab
# Usar IP en lugar de hostname si DNS del servidor es inestable
//10.42.10.61/Administrativa/UO01/CISO01/CISO01_CIMDESS  /mnt/dfs/CISO01_CIMDESS  cifs  credentials=/etc/smb-credentials-validador,uid=1000,gid=1000,file_mode=0660,dir_mode=0770,_netdev  0  0
```

`/etc/smb-credentials-validador`:

```ini
username=USUARIO_DOMINIO
password=CLAVE
domain=CENS
```

```bash
sudo mount -a
```

---

## 2) Publicar el montaje al contenedor

En `docker-compose.prod.yml` (ya alineado con la ruta del proyecto):

```yaml
volumes:
  - /mnt/dfs/CISO01_CIMDESS:/mnt/dfs/CISO01_CIMDESS
```

Host y contenedor deben ver la **misma ruta** bajo `/mnt/dfs/...`.

---

## 3) `.env` en el servidor Linux

```env
# Windows (solo referencia; en Linux no se usa si DATA_DIR_LINUX está definido)
DATA_DIR=\\CENS-AD02.cens.corp.epm.com.co\Administrativa\UO01\CISO01\CISO01_CIMDESS\S0022\data

# Ruta POSIX del montaje (sin hostname, sin \\)
DATA_DIR_LINUX=/mnt/dfs/CISO01_CIMDESS/S0022/data
```

**No hace falta** poner la IP dentro de `DATA_DIR_LINUX`. Solo cambie `DATA_DIR` en Windows si allá también falla el DNS:

```env
DATA_DIR=\\10.42.10.61\Administrativa\UO01\CISO01\CISO01_CIMDESS\S0022\data
```

---

## 4) Redeploy y comprobaciones

```bash
docker compose -f docker-compose.prod.yml up -d --force-recreate

# App viva (sin DFS)
curl -s http://localhost:8022/health/

# DFS visible dentro del contenedor
docker exec -it validador-xm-qe sh -lc 'echo DATA_DIR_LINUX=$DATA_DIR_LINUX && ls -la $DATA_DIR_LINUX'
```

En la web: **Herramientas → Gestión de Archivos** debe mostrar *Estado de acceso: Disponible* y listar carpetas QE / XM / Resultados.

Si el montaje en el host falla, verá `Host is down` o pantalla de error aunque Django arranque.

---

## 5) Resumen para el equipo de infra

1. Servidor SMB: IP `10.42.10.61`, share `Administrativa/.../CISO01_CIMDESS`.
2. Montar en el host Linux con **IP** o **/etc/hosts**.
3. Dejar `DATA_DIR_LINUX` como ruta local `/mnt/dfs/...`.
4. No bloquear DNS saliente solo para la app: el contenedor usa el filesystem montado por el host.

---

## Checklist rápido

- [ ] `mount | grep CISO01` muestra el recurso montado
- [ ] `ls /mnt/dfs/CISO01_CIMDESS/S0022/data` lista archivos
- [ ] `.env` del servidor tiene `DATA_DIR_LINUX` correcto
- [ ] `docker logs validador-xm-qe` sin `ImproperlyConfigured` ni crash al arrancar
- [ ] `curl http://servidor:8022/health/` → `ok`
