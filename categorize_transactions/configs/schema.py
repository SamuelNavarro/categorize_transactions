"""Data schema for the MiBolsillo dataset."""

data_schema = {
    'id': str,
    'celular': str,
    'safra_abertura': str,
    'cidade': str,
    'estado': str,
    'idade': 'Int64',
    'sexo': 'category',
    'limite_total': float,
    'limite_disp': float,
    'data': str,
    'valor': float,
    'grupo_estabelecimento': 'category',
    'cidade_estabelecimento': str,
    'pais_estabelecimento': str,
}
