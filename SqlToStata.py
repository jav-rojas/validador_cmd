
#1) Importa librerias
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
from pysftp import Connection as pysftpConnection
from pysftp import CnOpts as pysftpCnOpts
from os.path import join as ospathjoin
from tempfile import NamedTemporaryFile
import sys
from ast import literal_eval as astliteral_eval
from pymysql import Error as pymysqlError
from pymysql import connect as pymysqlconnect
from sqlalchemy import create_engine
import pandas as pd

##################################################################################
# Etapa 1: Definición de parámetros
##################################################################################

text_user   = "Datos2020"
pass_sql  = "Datos_CMD2020"
base_sql  = "EOD202003"
table_sql = "EOD202003"
query = 'SELECT * FROM EOD202003.EOD202003;'

##################################################################################
# Etapa 2: Descarga de base actual de SQL con cambios realizados por coordinadores
##################################################################################

# Descarga
def DescargaSql(text_user,pass_sql,base_sql,query):
    DataFull = None
    result = None
    try:     
        sql_conn = pymysqlconnect(host="162.243.165.69",
          port=3306,
          user=text_user,
          passwd=pass_sql,
          db=base_sql)
        
        try:
            DataFull = pd.read_sql(query, con=sql_conn)
        except:
            pass
            
        result = 'Completado'
        #Modificado por Javier: 3 de Abril de 2020
        return DataFull, result
    except pymysqlError as e:
        result = "Error en la conexión %d: %s" %(e.args[0], e.args[1])
        #Modificado por Javier: 3 de Abril de 2020
        return DataFull, result

# Base
sql, result = DescargaSql(text_user,pass_sql,base_sql,query)

##################################################################################
# Etapa 3: Eliminación de duplicados (solo permanece más actual)
##################################################################################

# Deja ultima obs
sql2 = sql.sort_values(by=['act']).drop_duplicates(subset=['interview__key','orden'], keep='last')
del sql2['level_0']

# Pasa variables de None a nan
sql2.loc[:,['act_eco1','actecon','institu']] = np.nan

# Carga base borrar y elimina obs
borrar = pd.read_stata('G:\\Mi unidad\\CMD\\Validador\\data\\borrar.dta')
sql2 = sql2.merge(borrar, on = 'interview__key', how = 'left', indicator = True)
sql2 = sql2.loc[sql2['_merge'] == 'left_only',:]
sql2 = sql2.drop(['_merge'], axis = 1)

# Eliminar 
ids_borrar = ['76-89-76-58','14-44-66-74','34-83-87-17','14-44-66-74','61-05-81-95','62-60-59-05','94-94-82-75']
borrar = pd.DataFrame(columns=['interview__key'], data = ids_borrar)
sql2 = sql2.merge(borrar, on = 'interview__key', how = 'left', indicator = True)
sql2 = sql2.loc[sql2['_merge'] == 'left_only',:]
sql2 = sql2.drop(['_merge'], axis = 1)

# Identificación
identificacion = pd.read_excel('G:\\Mi unidad\\CMD\\Validador\\data\\Identificación CORRECTA.xlsx')
identificacion = identificacion.rename(columns = {'Interview key':'interview__key'})
identificacion = identificacion.loc[:,['interview__key','segmento','hogar','idencuesta']]

sql2 = sql2.merge(identificacion, on = 'interview__key', how = 'left', indicator = True)
sql2.loc[:,'segmento_x'] = sql2['segmento_y']
sql2.loc[:,'hogar_x'] = sql2['hogar_y']
sql2.loc[:,'encuesta'] = sql2['idencuesta']

'''
# Comprobar si el cambio se realizó correctamente
sql2['ind'] = 0
sql2['ind2'] = 0
sql2['ind3'] = 0
sql2.loc[(sql2['segmento_x'] == sql2['segmento_y']),'ind'] = 1
sql2.loc[(sql2['hogar_x'] == sql2['hogar_y']),'ind2'] = 1
sql2.loc[:,'ind3'] = sql2['ind']+sql2['ind2']
ver = sql2.loc[sql2['ind3'] <2,:]
'''
sql2 = sql2.drop(['segmento_y','hogar_y','idencuesta'], axis = 1)
sql2 = sql2.rename(columns={'segmento_x':'segmento','hogar_x':'hogar','encuesta':'idencuesta'})
sql2 = sql2.drop(['_merge'], axis = 1)

sql2.to_stata("G:\\Mi unidad\\CMD\\EOD2\\EOD202003\\data\\Inputs\\EOD202003.dta", version = 117)

##################################################################################
# Etapa 4: Base completa a Drive
##################################################################################


myHostname = "162.243.165.69"
myUsername = "root"
myPassword = "DatosCMD2019"
filename = "BaseGeneral.dta"
opts = pysftpCnOpts()
opts.hostkeys = None
with pysftpConnection(host=myHostname, username=myUsername, password=myPassword, cnopts = opts) as sftp:
    remoteFilePath = '/root/CMD/EOD_Call/bases/validador/'+filename
    localFilePath = "G:\\Mi unidad\\CMD\\EOD2\\EOD202003\\data\\Inputs\\"+filename
    sftp.get(remoteFilePath, localFilePath)




