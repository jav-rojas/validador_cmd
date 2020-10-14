import pandas as pd
from os import path
from modules.sql_utils import conn_sql, dl_sql, keys_parser

##################################################################################
# Etapa 1: Definición de parámetros
##################################################################################

db = 'EOD202009'
keys = '''75-42-46-72
14-25-09-08
34-22-96-54
20-40-40-38
19-52-90-78
73-45-32-49
06-51-95-24
08-72-50-37
13-86-91-74
90-15-61-80
12-91-91-31
15-41-90-04
59-13-59-66'''

##################################################################################
# Etapa 2: Cambiar tipo_muestra a 0
##################################################################################

query = keys_parser().parse(keys, query=True, query_type='update', args_sql=[db, 'tipo_muestra', '0'])
query_execute = conn_sql(db=db)
query_execute.execute(query=query)
query_execute.commit_close_connection()

##################################################################################
# Etapa 3: Identificar hogares que presentan valores distintos en tipo_muestra
##################################################################################

# Base desde SQL
query = 'SELECT * FROM {}.{}'.format(db, db)
sql_tm, result = dl_sql(db=db).descarga(query=query, duplicates_level='interview__key')
sql_tm = sql_tm.loc[:, ['interview__key', 'tipo_muestra']]

# Base Final Isabel Seguel
isa_tm = pd.read_excel(path.join('data', 'input', 'eod202009_isa.xlsx'), sheet_name='BASE FINAL')
isa_tm = isa_tm.sort_values(by=['act']).drop_duplicates(subset=['interview__key'], keep='last')
isa_tm = isa_tm.loc[:, ['interview__key', 'tipo_muestra']]

# Compara ambas
compara = sql_tm.merge(isa_tm, on = 'interview__key')
compara = compara.loc[compara['tipo_muestra_x'] != compara['tipo_muestra_y'], :]