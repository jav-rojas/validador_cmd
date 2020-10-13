import pandas as pd
from os import path
from modules.sql_utils import conn_sql

##################################################################################
# Etapa 1: Parámetros
##################################################################################

db = 'EOD202009'

##################################################################################
# Etapa 2: Cargamos input de hogares nuevos a SQL
##################################################################################

nuevos = pd.read_csv(path.join('data', 'input', 'hogares_nuevos.csv'), delimiter=';')
key = nuevos.interview__key.values.tolist()
encuesta = nuevos.encuesta.values.tolist()

for i in range(0, len(key)):
    query = "UPDATE {}.{} SET encuesta = {}, idencuesta = {} WHERE interview__key = '{}'".format(db, db, encuesta[i], encuesta[i], key[i])
    obj = conn_sql(db=db)
    obj.execute(query=query)
    obj.commit_close_connection()
