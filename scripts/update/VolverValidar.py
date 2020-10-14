import pymysql
import pandas as pd
import os

##################################################################################
# Etapa 1: Definición de parámetros
##################################################################################

text_user   = "Datos2020"
pass_sql  = "Datos_CMD2020"
base_sql  = "EOD202006"
table_sql = "EOD202006"
dir_volver_validar = 'G:\\Mi unidad\\CMD\\Validador\\data\\volver_validar'
estado = 0

################################################################################
# Etapa 2: Crear lista de IK para los que se reiniciará estado a 0
################################################################################

volver_validar = pd.read_excel(os.path.join(dir_volver_validar,'EOD202006 - 29-07-2020.xlsx'))
volver_validar = volver_validar.interview__key.values.tolist()
volver_validar = list(dict.fromkeys(volver_validar))
separator = ','
ik_volver_validar = separator.join(volver_validar)

###################################################################################
# Etapa 3: Realizamos el cambio del estado de la encuesta sobre los IK 
###################################################################################

resultado = {}

def ejecutar_queries(queries):
    sql_conn = pymysql.connect(host="162.243.165.69",
                  port=3306,
                  user=text_user,
                  passwd=pass_sql,
                  db=base_sql)

    # Inicia cursor:    
    cursor = sql_conn.cursor()
    
    for i in range(0,len(queries)):
        # Número filas afectadas
        rows = cursor.execute(queries[i][0])
        # Resultado de query (filas)
        result = cursor.fetchall()
        # Agregamos resultado a diccionario
        resultado['{}'.format(queries[i][1])] = {}
        resultado['{}'.format(queries[i][1])]['Filas afectadas'] = rows
        resultado['{}'.format(queries[i][1])]['Resultado'] = result
    
    # Nos aseguramos de que se realicen los cambios, y cerramos la conexión
    sql_conn.commit()
    cursor.close()
    sql_conn.close()

queries = [('SET @a = "{}";'.format(ik_volver_validar),'1. Creación de variable local en SQL con IKeys'),\
           ('SELECT @a;','2. Mostrar variable local creada'),\
           ('UPDATE {}.{} SET estado = {} WHERE FIND_IN_SET(interview__key, @a);'.format(base_sql,table_sql,estado),'3. Cambiar estado de IKeys en variable local creada')]

ejecutar_queries(queries)

#query = 'SET @a := (SELECT GROUP_CONCAT(DISTINCT interview__key) FROM PruebaEOD.volver_validar);'
#query = 'SET @a = {};'.format(ik_volver_validar)
#query = 'SET global group_concat_max_len=15000;'
#query = 'SELECT * FROM PruebaEOD.PruebaEOD WHERE FIND_IN_SET(interview__key, @a);'
