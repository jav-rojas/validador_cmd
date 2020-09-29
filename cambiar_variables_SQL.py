import pymysql
import pandas as pd
import os
import numpy as np

##################################################################################
# Etapa 1: Definición de parámetros
##################################################################################

text_user   = "Datos2020"
pass_sql  = "Datos_CMD2020"
base_sql  = "EOD202006"
table_sql = "EOD202006"
dir_cambiar_sql = 'G:\\Mi unidad\\CMD\\Validador\\data\\'
estado = 0

################################################################################
# Etapa 2: Crear lista de IK para los que se reiniciará estado a 0
################################################################################

cambiar_sql = pd.read_excel(os.path.join(dir_cambiar_sql,'cambiar_sql.xlsx'))
cambiar_sql = cambiar_sql.where((pd.notnull(cambiar_sql)), 'NULL')
interview__key_sql = cambiar_sql.interview__key.values.tolist()
orden_sql = cambiar_sql.orden.values.tolist()
pregunta_sql = cambiar_sql.pregunta.values.tolist()
valor_sql = cambiar_sql.valor_surveysolutions.values.tolist()
#cambiar_sql = list(dict.fromkeys(cambiar_sql))

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
        print(i)        
        print(queries[i][0])
        # Número filas afectadas
        rows = cursor.execute(queries[i][0])
        # Resultado de query (filas)
        result = cursor.fetchall()
        
        print(result)
        # Agregamos resultado a diccionario
        resultado['{}'.format(queries[i][1])] = {}
        resultado['{}'.format(queries[i][1])]['Filas afectadas'] = rows
        resultado['{}'.format(queries[i][1])]['Resultado'] = result
    
    # Nos aseguramos de que se realicen los cambios, y cerramos la conexión
    sql_conn.commit()
    cursor.close()
    sql_conn.close()

queries = []

'''
for i in range(0,len(interview__key_sql)):
    add = "SELECT {} FROM restore_backup.EOD202006 WHERE interview__key = '{}' AND orden = '{}';".format(pregunta_sql[i],interview__key_sql[i],orden_sql[i])
    queries.append((add,'ik-'+interview__key_sql[i]+'-o-'+str(orden_sql[i])+'-preg-'+str(pregunta_sql[i])))

'''
for i in range(0,len(interview__key_sql)):
    if type(valor_sql[i]) == str and valor_sql[i] != 'NULL':
        add = "UPDATE EOD202006.EOD202006 SET {} = '{}' WHERE interview__key = '{}' AND orden = '{}';".format(pregunta_sql[i],valor_sql[i],interview__key_sql[i],orden_sql[i])
        queries.append((add,'ik-'+interview__key_sql[i]+'-o-'+str(orden_sql[i])+'-preg-'+str(pregunta_sql[i])))
    elif type(valor_sql[i]) == str and valor_sql[i] == 'NULL':
        add = "UPDATE EOD202006.EOD202006 SET {} = {} WHERE interview__key = '{}' AND orden = '{}';".format(pregunta_sql[i],valor_sql[i],interview__key_sql[i],orden_sql[i])
        queries.append((add,'ik-'+interview__key_sql[i]+'-o-'+str(orden_sql[i])+'-preg-'+str(pregunta_sql[i])))        
    if type(valor_sql[i]) == int:
        add = "UPDATE EOD202006.EOD202006 SET {} = {} WHERE interview__key = '{}' AND orden = '{}';".format(pregunta_sql[i],valor_sql[i],interview__key_sql[i],orden_sql[i])
        queries.append((add,'ik-'+interview__key_sql[i]+'-o-'+str(orden_sql[i])+'-preg-'+str(pregunta_sql[i])))
 
ejecutar_queries(queries)

###################################################################################
# Etapa 4: Solo para cuando se utiliza SELECT como query 
###################################################################################

results = list(resultado.keys())

len(resultado[results[14]]['Resultado'])

results_preg = []

for i in range(0,len(results)):
    aux = resultado[results[i]]['Resultado']
    for j in range(0,len(aux)):
        a = resultado[results[i]]['Resultado'][j][0]
        results_preg.append((interview__key_sql[i],orden_sql[i],pregunta_sql[i],a))

variables = ['interview__key','orden','pregunta','valor_SQL']
df_cambios = pd.DataFrame(columns = variables, index = np.arange(len(results_preg)))

for i in range(0,len(results_preg)):
    for j in range(0,len(variables)):
        element = results_preg[i][j]
        df_cambios.iloc[i,j] = element

df_cambios.to_excel(dir_cambiar_sql + 'base_consulta_query.xlsx', index = False)














