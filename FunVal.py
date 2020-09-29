# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 21:37:41 2020

@author: usuario
"""

from ast import literal_eval as astliteral_eval
from pymysql import Error as pymysqlError
from pymysql import connect as pymysqlconnect
from sqlalchemy import create_engine
import pandas as pd

def MyConverter(mydata):
    def cvt(data):
        try:
            return astliteral_eval(data)
        except Exception:
            return str(data)
    return tuple(map(cvt, mydata))

###############################################################################
# Función para descar base de datos en SQL
###############################################################################    

#Modificado por Javier: 3 de Abril de 2020: función agregada
def EjecutarQuery(text_user,pass_sql,base_sql,query):
  
    try:     
        sql_conn = pymysqlconnect(host="162.243.165.69",
          port=3306,
          user=text_user,
          passwd=pass_sql,
          db=base_sql)
        
    except pymysqlError as e:
        result = "Error en la conexión %d: %s" %(e.args[0], e.args[1])
        return result
    
    cursor = sql_conn.cursor()
    cursor.execute(query)


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


def SqlAlchemyEngine(text_user,pass_sql,base_sql):
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@162.243.165.69:3306/{db}"
                           .format(user=text_user,
                                   pw=pass_sql,
                                   db=base_sql))
    
    return engine

def DescargaUltSql(text_user,pass_sql,base_sql,table_sql,self, list_des = ""):
    
    #base_sql  = "EOD202003"
    #table_sql = "EOD202003"
    #pass_sql= 'Datos_CMD2020'
    #text_user = 'Datos2020'
    # query = 'SELECT * FROM EOD202003.EOD202003;'
    query = 'DROP TABLE IF EXISTS aux0;'
    DataFull, result = DescargaSql(text_user,pass_sql,base_sql,query)

    query = 'CREATE TABLE aux0 AS SELECT interview__key, max(act) as max_act FROM {tabla1} GROUP BY interview__key;'.format(tabla1=base_sql+'.'+table_sql)
    DataFull, result = DescargaSql(text_user,pass_sql,base_sql,query)

    query = 'SELECT {tabla1}.* FROM {tabla1} LEFT JOIN {tabla2} ON {tabla1}.interview__key={tabla2}.interview__key WHERE {tabla1}.act = {tabla2}.max_act;'.format(tabla1=base_sql+'.'+table_sql,  tabla2='aux0')
    DataFull, result = DescargaSql(text_user,pass_sql,base_sql,query)

#    query = 'SELECT {tabla1}.* FROM {tabla1}'.format(tabla1=base_sql+'.'+table_sql,  tabla2='aux0')
#    DataFull, result = DescargaSql(text_user,pass_sql,base_sql,query)
    
#ver=DataFull.loc[DataFull.interview__key=="17-32-74-70" , : ]
#ver.loc[: , 'telefono2' ] = 24
#engine = SqlAlchemyEngine(text_user,pass_sql,base_sql)
#ver.loc[:,'act'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#ver.to_sql('EOD2020203', con = engine, if_exists = 'append', chunksize = 10, index = False)

    query = 'SELECT DISTINCT cuarto FROM {tabla1};'.format(tabla1=base_sql+'.'+table_sql)
    List, result = DescargaSql(text_user,pass_sql,base_sql,query)
    
    
    DataFull = None
    
    #self.progress.setMaximum(100)
    tot = len(List)
    for i in range(0, len(List)):
        
        if i == 0:
            i=0       
            query = 'SELECT {tabla1}.* FROM {tabla1} LEFT JOIN {tabla2} ON {tabla1}.interview__key={tabla2}.interview__key WHERE {tabla1}.act = {tabla2}.max_act AND {tabla1}.cuarto={s};'.format(tabla1=base_sql+'.'+table_sql,  tabla2='aux0', s=List.iloc[i,0])
            DataFull, result = DescargaSql(text_user,pass_sql,base_sql,query)
            #print(len(DataFull))
            #return self.progress.setValue(i)
        
        if i>0:
            query = 'SELECT {tabla1}.* FROM {tabla1} LEFT JOIN {tabla2} ON {tabla1}.interview__key={tabla2}.interview__key WHERE {tabla1}.act = {tabla2}.max_act AND {tabla1}.cuarto={s};'.format(tabla1=base_sql+'.'+table_sql,  tabla2='aux0', s=List.iloc[i,0])
            DataFull0, result = DescargaSql(text_user,pass_sql,base_sql,query)
            DataFull=DataFull.append(DataFull0)
            #return self.progress.setValue(i)
        
        self.countChanged.emit(int(i/(tot-1)*100))
        #return DataFull
        #print(i)

    df_or = DataFull.loc[DataFull['tipo_muestra'] == 1,:]

    if list_des != "":
        DataFull = DataFull.loc[DataFull['tipo_muestra'].isin(list_des)]

    return DataFull, result, df_or