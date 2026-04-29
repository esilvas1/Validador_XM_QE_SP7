import os
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import RequestFactory, SimpleTestCase, override_settings

from .views import check_sp7_previo


class CheckSp7PrevioTests(SimpleTestCase):
	def setUp(self):
		self.factory = RequestFactory()

	def test_uses_effective_data_dir_from_settings_not_environment(self):
		with TemporaryDirectory() as temp_dir:
			reporte_dir = Path(temp_dir) / 'Reporte Mensual'
			reporte_dir.mkdir(parents=True)

			archivo = reporte_dir / 'Consulta_Eventos_Cens_Apertura_Cierre_2026_02.csv'
			archivo.write_text('columna1\nvalor1\n', encoding='utf-8')

			original_data_dir = os.environ.get('DATA_DIR')
			os.environ['DATA_DIR'] = r'\\servidor-invalido\dfs\data'

			try:
				request = self.factory.get('/procesos/check-sp7-previo/')
				with override_settings(DATA_DIR=Path(temp_dir)):
					response = check_sp7_previo(request)

				self.assertEqual(response.status_code, 200)
				payload = json.loads(response.content.decode('utf-8'))
				self.assertJSONEqual(
					response.content.decode('utf-8'),
					{
						'found': True,
						'filename': 'Consulta_Eventos_Cens_Apertura_Cierre_2026_02.csv',
						'fecha_mod': payload['fecha_mod'],
						'size_kb': payload['size_kb'],
						'carpeta': str(reporte_dir),
						'periodo': 'Detectado en DFS',
					},
				)
			finally:
				if original_data_dir is None:
					os.environ.pop('DATA_DIR', None)
				else:
					os.environ['DATA_DIR'] = original_data_dir
