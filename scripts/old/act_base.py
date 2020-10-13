# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 17:53:50 2020

@author: Diego Menares

Este codigo toma la base generada en el FTP y la carga a SQL en el cual el
valiador de conecta. Solo va actualizando los cdf=110 y interview__key nuevos.
"""

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

df_comentarios = sql.drop_duplicates('comentarios_validacion', keep="last")    
df_comentarios = sql.drop_duplicates('mes', keep="last")    

# Creamos una lista con los comentarios de la validación
comentarios_split = df_comentarios['comentarios_validacion']
comentarios_split = comentarios_split.values.tolist()[0]
list_comentario = comentarios_split.split("___")    

# Creamos una lista con los comentarios de SurveySolutions
list_comentario_2 = df_comentarios['p7']
list_comentario_2 = list_comentario_2.values.tolist()[0]
list_comentario_2 = [list_comentario_2]

largo_comentarios = len(list_comentario) + len(list_comentario_2)

# Creamos dataframe:
df_comentarios = pd.DataFrame(index=np.arange(0, largo_comentarios), columns = ['Comentario','Origen'])
# Agregamos comentarios:
df_comentarios.iloc[0,0] = list_comentario_2[0]
df_comentarios.iloc[0,1] = "Survey Solutions"

for i in range(0,len(list_comentario)):
    j= i+1
    df_comentarios.iloc[j,0] = list_comentario[i] 
    df_comentarios.iloc[j,1] = 'Validación'




sql.to_excel("G:\\Mi unidad\\CMD\\Validador\\data\\respaldo_sql_18may.xlsx", index = False)

a = sql.dtypes
##################################################################################
# Etapa 3: Descarga de base con datos actualizados de codificación
##################################################################################

#Esta es necesaria para consultas con pandas
engine = create_engine("mysql+pymysql://{user}:{pw}@162.243.165.69:3306/{db}"
                       .format(user=text_user,
                               pw="Datos_CMD2020",
                               db=base_sql))

# Por ahora tomaré una base desde mi computador local
f  =  NamedTemporaryFile(suffix="BaseValidador", delete = False)

myHostname = "162.243.165.69"
myUsername = "root"
myPassword = "DatosCMD2019"
filename = "BaseValidador.dta"
opts = pysftpCnOpts()
opts.hostkeys = None
with pysftpConnection(host=myHostname, username=myUsername, password=myPassword, cnopts = opts) as sftp:
    remoteFilePath = '/root/CMD/EOD_Call/bases/validador/'+filename
    localFilePath = ospathjoin(f.name)
    sftp.get(remoteFilePath, localFilePath)

sys.path.append(f.name)
vps = pd.read_stata(f, convert_categoricals=False)

##################################################################################
# Etapa 4: Union de base VPS a SQL
##################################################################################

#############
### VPS
#############

# Cambiamos de formato orden
vps['orden'] = vps['orden'].astype('str').str[:-2].astype('float64')

# Dejamos las columnas que nos sirven
columnas = ['interview__key','nombre','orden','oficio','p9_cod1','p11a_cod','p11a_tab','p11b_cod','p11b_tab']
vps2 = vps.loc[:,columnas]

#############
### SQL
#############

# Dejamos las columnas que nos sirven
columnas = ['interview__key','nombre','orden','act','oficio','p9_cod1','p11a_cod','p11a_tab','p11b_cod','p11b_tab']
sql2 = sql.loc[:,columnas]

#############
### SQL+VPS
#############

base = sql2.merge(vps2, on = ['interview__key','orden','nombre'], how = 'left')
col1 = ['oficio_x','p9_cod1_x','p11a_cod_x','p11a_tab_x','p11b_cod_x','p11b_tab_x']
col2 = ['oficio_y','p9_cod1_y','p11a_cod_y','p11a_tab_y','p11b_cod_y','p11b_tab_y']

for i in range(0,len(col1)):
    base['{}'.format(col1[i])] = base['{}'.format(col2[i])]

col3 = ['interview__key','nombre','orden','act','oficio_x','p9_cod1_x','p11a_cod_x','p11a_tab_x','p11b_cod_x','p11b_tab_x']
base = base.loc[:,col3]
base = base.rename(columns={'oficio_x':'oficio',\
                             'p9_cod1_x':'p9_cod1',\
                             'p11a_cod_x':'p11a_cod',\
                             'p11a_tab_x':'p11a_tab',\
                             'p11b_cod_x':'p11b_cod',\
                             'p11b_tab_x':'p11b_tab'})

##########################################
### Etapa 5: Actualizar base SQL en Python
##########################################

sql = sql.drop(['oficio','p9_cod1','p11a_cod','p11a_tab','p11b_cod','p11b_tab'], axis = 1)
sql = sql.merge(base, on = ['interview__key','orden','nombre','act'], how = 'left')

#######################################
### Etapa 6: Actualizar base SQL online
#######################################

#Esta es necesaria para consultas con pandas

engine = create_engine("mysql+pymysql://{user}:{pw}@162.243.165.69:3306/{db}"
                       .format(user=text_user,
                               pw="Datos_CMD2020",
                               db=base_sql))

result=sql.to_sql(table_sql, con = engine, chunksize = 1000,if_exists='replace')

