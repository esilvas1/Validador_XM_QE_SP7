"""
Exige sesión de Herramientas para todas las rutas de validacion_app salvo
portada del dashboard y login/cierre de sesión.

Nota: la comprobación va en process_view (no en __call__) porque
request.resolver_match solo está disponible después de resolver la URL.
"""
import urllib.parse

from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.deprecation import MiddlewareMixin


class HerramientasLoginRequiredMiddleware(MiddlewareMixin):
    PUBLIC_URL_NAMES = frozenset({
        'index',
        'dashboard_sp7',
        'dashboard_sp7_data',
        'herramientas_login',
        'herramientas_logout',
    })

    def process_view(self, request, view_func, view_args, view_kwargs):
        match = getattr(request, 'resolver_match', None)
        if not match or not match.url_name:
            return None
        if getattr(match, 'app_name', None) != 'validacion_app':
            return None
        if match.url_name in self.PUBLIC_URL_NAMES:
            return None
        if request.session.get('herramientas_user'):
            return None
        return self._deny(request, match)

    def _deny(self, request, match):
        if match.url_name and str(match.url_name).endswith('_stream'):
            return HttpResponse('No autorizado\n', status=401, content_type='text/plain; charset=utf-8')

        accept = (request.headers.get('Accept') or '').lower()
        if 'text/event-stream' in accept:
            return HttpResponse('No autorizado\n', status=401, content_type='text/plain; charset=utf-8')

        wants_json = (
            request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            or 'application/json' in accept
        )
        if wants_json or request.method != 'GET':
            return JsonResponse(
                {'success': False, 'message': 'Sesión requerida. Inicie sesión en Herramientas.'},
                status=401,
            )

        login_url = reverse('validacion_app:herramientas_login')
        next_path = request.get_full_path()
        return redirect(f'{login_url}?next={urllib.parse.quote(next_path)}')
