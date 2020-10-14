import pandas as pd
from os import path
from modules.sql_utils import conn_sql, dl_sql

##################################################################################
# Etapa 1: Parámetros
##################################################################################

db = 'EOD202009'

##################################################################################
# Etapa 2: Cargamos input de hogares nuevos a SQL
##################################################################################

# Carga idencuesta a nuevos hogares
nuevos = pd.read_csv(path.join('data', 'input', 'hogares_nuevos.csv'), delimiter=';')
key = nuevos.interview__key.values.tolist()
encuesta = nuevos.encuesta.values.tolist()

for i in range(0, len(key)):
    query = "UPDATE {}.{} SET encuesta = {}, idencuesta = {} WHERE interview__key = '{}'".format(db, db, encuesta[i], encuesta[i], key[i])
    obj = conn_sql(db=db)
    obj.execute(query=query)
    obj.commit_close_connection()

##################################################################################
# Etapa 3: Hogares sin identificación
##################################################################################

# Carga Base Validada actual y detecta hogares sin identificación
completa = pd.read_excel(path.join('data', 'output', 'BaseValidada.xlsx'))
nuevos = completa.loc[(completa['tipo_muestra'] == 2) & (completa['cuarto'] == 100), :]
nuevos = nuevos.sort_values(by=['act']).drop_duplicates(subset=['interview__key'], keep='last')
idencuestas = nuevos.loc[:, ['interview__key', 'encuesta']]

# Carga relación entre hogares nuevos y originales
relacion = pd.read_csv(path.join('data', 'input', 'relacion_hogares_nuevos.csv'), delimiter=';')

# Merge entre ambas bases
identificar = idencuestas.merge(relacion, on='interview__key', how='left')  # 39-01-91-47 no tiene original

# Busca información de identificación de hogares originales
ikeys_df = idencuestas.merge(relacion, on='interview__key')
ikeys = str(tuple(set(ikeys_df.original.values.tolist())))
variables = "interview__key, cuarto, estrato, comuna, segmento, hogar, encuesta, gse, comunat, direccion, coordinador, obs, nombre_enc, jefe_hogar, act"
query = "SELECT {} FROM {}.{} WHERE interview__key in {}".format(variables, db, db, ikeys)
informacion, result = dl_sql(db=db).descarga(query=query, duplicates_level='interview__key')
aux = pd.merge(ikeys_df, informacion, left_on='original', right_on='interview__key', how='left')  # 07-69-07-49 no está en sql
informacion_ikeys = pd.merge(ikeys_df, informacion, left_on='original', right_on='interview__key')

# A excel
informacion_ikeys.to_excel(path.join('data', 'output', 'informacion_hogares_nuevos_preliminar.xlsx'), index=False)

##################################################################################
# Etapa 3: Cargamos informacion de hogares nuevos a SQL
##################################################################################

# Carga informacion a nuevos hogares
informacion_ikeys = pd.read_excel(path.join('data', 'output', 'informacion_hogares_nuevos.xlsx'))
key = informacion_ikeys.interview__key.values.tolist()
cuarto = informacion_ikeys.cuarto.values.tolist()
estrato = informacion_ikeys.estrato.values.tolist()
comuna = informacion_ikeys.comuna.values.tolist()
segmento = informacion_ikeys.segmento.values.tolist()
hogar = informacion_ikeys.hogar.values.tolist()
gse = informacion_ikeys.gse.values.tolist()
comunat = informacion_ikeys.comunat.values.tolist()
direccion = informacion_ikeys.direccion.values.tolist()
coordinador = informacion_ikeys.coordinador.values.tolist()
obs = informacion_ikeys.obs.values.tolist()
nombre_enc = informacion_ikeys.nombre_enc.values.tolist()
jefe_hogar = informacion_ikeys.jefe_hogar.values.tolist()

variables = [cuarto, estrato, comuna, segmento, hogar, gse, comunat, direccion, coordinador, obs, nombre_enc, jefe_hogar]
nombres = ['cuarto', 'estrato', 'comuna', 'segmento', 'hogar', 'gse', 'comunat', 'direccion', 'coordinador', 'obs', 'nombre_enc', 'jefe_hogar']

for i in range(0, len(key)):
    for j in range(0, len(variables)):
        print(key[i], nombres[j], variables[j][i])
        query = "UPDATE {}.{} SET {} = '{}' WHERE interview__key = '{}'".format(db, db, nombres[j], variables[j][i], key[i])
        obj = conn_sql(db=db)
        obj.execute(query=query)
        obj.commit_close_connection()
