import pandas as pd
from os import path
from modules.sql_utils import dl_sql

##################################################################################
# Etapa 1: Parámetros
##################################################################################

db = 'EOD202009'

##################################################################################
# Etapa 2: Descarga
##################################################################################

query = 'SELECT * FROM {}.{}'.format(db, db)
sql, result = dl_sql(db=db).descarga(query=query, duplicates_level='orden')

##################################################################################
# Etapa 3: Bases
##################################################################################

# Limpieza
sql.drop(['otro_codigo', 'just_rechazo', 'sex_rechazo', 'p15a_cod', 'p15b_cod', 'tpodes'], axis='columns', inplace=True)

# Base Validada
sql.to_stata(path.join('data', 'output', 'BaseValidada.dta'), write_index=False, version=None)
sql.to_excel(path.join('data', 'output', 'BaseValidada.xlsx'), index=False)

# Base Validada Final
sql = sql.loc[sql['tipo_muestra'].isin([1, 2])]
sql.to_stata(path.join('data', 'output', 'BaseValidada_vf.dta'), write_index=False, version=None)
sql.to_excel(path.join('data', 'output', 'BaseValidada_vf.xlsx'), index=False)
