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

sql2 = sql.sort_values(by=['act']).drop_duplicates(subset=['interview__key','orden'], keep='last')

################################################################################
# Etapa 4: Descarga base validador
################################################################################

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
bd = pd.read_stata(f, convert_categoricals=False)
bd = bd.loc[:,['interview__key','orden']]

################################################################################
# Etapa 5: Compara base validador y base sql
################################################################################

a1 = sql2.merge(bd, on = ['interview__key','orden'], how = 'left', indicator = True)
a2 = sql2.merge(bd, on = ['interview__key','orden'], how = 'right', indicator = True)
a3 = sql2.merge(bd, on = ['interview__key','orden'], how = 'outer', indicator = True)
a4 = sql2.merge(bd, on = ['interview__key','orden'], how = 'inner', indicator = True)

################################################################################
# Etapa 6: Exporta a excel/csv
################################################################################

# No match
no_match = a1.loc[a1._merge == 'left_only',:]
no_match.to_csv('G:\\Mi unidad\\CMD\\Validador\\data\\no_match.csv', index = False)

# Match
match = a1.loc[a1._merge == 'both',:]
match = match.drop(['level_0','index','_merge'], axis = 1)
match = match.drop_duplicates(subset = ['interview__key','orden'])

# Para Isa
match.to_excel('G:\\Mi unidad\\CMD\\Validador\\data\\base_validador.xlsx', index = False)
# Para Diego
match.to_csv('G:\\Mi unidad\\CMD\\Validador\\Validador_v4\\Base SQL\\output_validador.csv', index = False)

