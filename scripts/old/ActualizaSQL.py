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

################################################################################
# input 0 
################################################################################

usuario   = "Datos2020"
password  = "Datos_CMD2020"
base_sql  = "EOD202009"
table_sql = "EOD202009"


# Fija parametros de la conexión

#Esta es necesaria para consultas normales
sql_conn = pymysql.connect(host="162.243.165.69",
              port=3306,
              user=usuario,
              passwd=password,
              db=base_sql)

#Esta es necesaria para consultas con pandas
engine = create_engine("mysql+pymysql://{user}:{pw}@162.243.165.69:3306/{db}"
                       .format(user=usuario,
                               pw=password,
                               db=base_sql))

################################################################################
# Etapa 1 Carga base de datos a python
################################################################################

# Por ahora tomaré una base desde mi computador local
f  =  NamedTemporaryFile(suffix="BaseValidador", delete = False)

myHostname = "162.243.165.69"
myUsername = "root"
myPassword = "DatosCMD2019"
filename = "BaseValidador.csv"
opts = pysftpCnOpts()
opts.hostkeys = None
with pysftpConnection(host=myHostname, username=myUsername, password=myPassword, cnopts = opts) as sftp:
    remoteFilePath = '/root/CMD/EOD_S20/bases/'+filename
    localFilePath = ospathjoin(f.name)
    sftp.get(remoteFilePath, localFilePath)

#sys.path.append(f.name)
#bd = pd.read_stata(ospathjoin(f.name), convert_categoricals=False)
#bd.to_csv(ospathjoin(f.name))
#bd = pd.read_csv(ospathjoin(f.name))

bd = pd.read_csv(ospathjoin(f.name))

bd["comentarios_validacion"] = "No hay comentarios de validación"

    
bd=bd.loc[bd.interview__key.notnull()]
bd.loc[:,'act']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
bd2=bd

#del bd2['p18g']
#bd2['p18g'] = bd2['p18g'].str.replace('\W', '')
#bd2['direccion'] = bd2['direccion'].str.replace('\W', '')

bd2['ntrab2'] = pd.to_numeric(bd2['ntrab2'], errors='coerce')
bd2['ntrab2'] = bd2['ntrab2'].astype('float64')
#Crea variable estado
bd2['estado']=0
bd2=bd2.loc[(bd2['interview__status']==120) | (bd2['interview__status']==130)]
#bd2=bd2.loc[bd2['tipo_muestra']==1]

try:
    del bd2["_merge"]
except:
    pass
################################################################################
# Etapa 2 Comprueba si la base existe
################################################################################

#Corrobora si tabla existe
# Muestra tablas
with sql_conn:
    cur = sql_conn.cursor()
    query = 'SHOW TABLES;'
    cur.execute(query)
    result = cur.fetchall()

contiene = 0 
for item in result:
   if table_sql in item:
       contiene = 1

################################################################################
# Etapa 3 Si exista solo carga observaciones nuevas
################################################################################

if contiene == 1:
    
    # Descargara base de datos desde SQL (para actualizar campos)
    bd_ik = pd.read_sql('SELECT DISTINCT interview__key FROM {tabla}'.format(tabla=base_sql+'.'+table_sql), con=sql_conn)
    #===> IMPORTANTE SALVAR VARIALBE CDF
    
    #Deja solo registros con cdf 110
    #bd_ik2=bd_ik[bd_ik['cdf'] == cdf_save]  #===> Actualizar nombre de variable cdf
    bd_ik2=bd_ik
    
    #Junto bases para dejar solo registros que estan en base bd2 (nuevos registros no SQL)
    bd3=bd2.merge(bd_ik2, on='interview__key', indicator=True,how='outer')
    bd4=bd3[bd3['_merge'] == "left_only"]
    bd4.loc[bd4['gse'] == '.', 'gse'] = '0'
    #bd4 = bd4.drop(['p5c_c_antes'], axis = 1)
    del bd4["_merge"]
    del bd4["sup"]
    del bd4["comentario_sup"]

    con = bd4['folio'].str.find('_c') ==4
    bd4.loc[con,'folio'] = bd4.loc[con,'folio'].str.replace('_c','00')
    bd4['folio'] = bd4['folio'].astype(int)
    
    
    #bd2['ntrab2'].astype(int).max()
    result=bd4.to_sql(table_sql, con = engine, if_exists = 'append', chunksize = 1000)

################################################################################
# Etapa 4 Si no existe carga base completa
################################################################################

if contiene == 0:     
    #result=bd2.to_sql(table_sql, con = engine, chunksize = 1000)
    result=bd2.to_sql(table_sql, con = engine, chunksize = 1000,if_exists='replace')


    
#Descaga base de datos
#DataFull= pd.read_sql('SELECT * FROM {tabla}'.format(tabla=base_sql+'.'+table_sql), con=sql_conn)

#DataFull['ntrab2'].dtype

#DataFull= pd.read_sql('SELECT * WHERE interview__key = {ik} FROM {tabla}'.format(tabla=base_sql+'.'+table_sql), ik=,  con=sql_conn)
    
#DataFull= pd.read_sql('SELECT * FROM {tabla}'.format(tabla=base_sql+'.'+table_sql), con=sql_conn)
#DataFull.to_csv("C:\\Users\\usuario\\Downloads\\BaseValidador20200518.csv")
#DataFull.to_csv("C:\\Users\\usuario\\Downloads\\BaseValidador20200519_2.csv")
    
    