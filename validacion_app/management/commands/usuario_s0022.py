import getpass

from django.core.management.base import BaseCommand, CommandError

from validacion_app.oracle_auth import (
    actualizar_password,
    generar_hash_bcrypt,
    insertar_usuario,
    obtener_hash_password,
)


class Command(BaseCommand):
    help = (
        'Gestiona usuarios de Herramientas en Oracle (S0022_USUARIOS): '
        'contraseña almacenada con bcrypt. Ejemplo: usuario_s0022 crear mi_usuario'
    )

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest='accion', required=True)
        p_ins = sub.add_parser('crear', help='INSERT: nuevo usuario y contraseña (hash bcrypt).')
        p_ins.add_argument('usuario', type=str)
        p_upd = sub.add_parser('actualizar', help='UPDATE: cambiar contraseña de un usuario existente.')
        p_upd.add_argument('usuario', type=str)

    def handle(self, *args, **options):
        accion = options['accion']
        usuario = (options.get('usuario') or '').strip()
        if not usuario:
            raise CommandError('Indique un nombre de usuario.')

        p1 = getpass.getpass('Contraseña: ')
        p2 = getpass.getpass('Repita contraseña: ')
        if p1 != p2:
            raise CommandError('Las contraseñas no coinciden.')
        if not p1:
            raise CommandError('La contraseña no puede estar vacía.')

        hash_b = generar_hash_bcrypt(p1)

        if accion == 'crear':
            if obtener_hash_password(usuario):
                raise CommandError(
                    f'El usuario "{usuario}" ya existe. Use: usuario_s0022 actualizar {usuario}'
                )
            try:
                insertar_usuario(usuario, hash_b)
            except Exception as e:
                raise CommandError(f'Error al insertar en Oracle: {e}') from e
            self.stdout.write(self.style.SUCCESS(f'Usuario "{usuario}" creado en S0022_USUARIOS.'))

        elif accion == 'actualizar':
            if not obtener_hash_password(usuario):
                raise CommandError(
                    f'No existe el usuario "{usuario}". Use: usuario_s0022 crear {usuario}'
                )
            try:
                n = actualizar_password(usuario, hash_b)
            except Exception as e:
                raise CommandError(f'Error al actualizar Oracle: {e}') from e
            if n == 0:
                raise CommandError('No se actualizó ninguna fila.')
            self.stdout.write(self.style.SUCCESS(f'Contraseña actualizada para "{usuario}".'))
