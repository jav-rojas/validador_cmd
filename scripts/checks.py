from os import path
from modules.sql_utils import dl_sql, keys_parser

##################################################################################
# Etapa 1: Definición de parámetros
##################################################################################

db = 'EOD202009'
controles_originales = '''22-99-81-88
99-48-88-94
32-16-51-88
82-19-88-18
01-80-91-13
62-61-00-60
83-16-24-66
84-22-61-48
86-59-00-16
99-22-48-56
16-29-86-25
84-38-95-82
55-59-37-19
72-72-37-21
09-45-13-29
95-45-27-05
43-74-87-00
02-95-77-44
99-49-12-79
09-87-40-32
39-05-86-08
79-42-34-26
90-93-12-05
20-32-28-57
70-88-08-62
64-62-58-08
89-47-43-06
61-14-93-48
56-72-61-15
77-85-26-01
77-49-45-62
66-42-72-75
93-60-50-11
94-57-83-48
78-93-30-78
92-51-67-17
68-08-74-56
04-40-59-26
82-33-24-48
95-60-61-01
78-50-82-50
15-30-86-94
28-75-20-47
83-48-97-42
07-64-20-36
90-65-79-05
56-22-99-93
68-38-57-30
85-92-24-21
21-32-91-99
59-27-56-34
10-11-50-52
37-56-76-77
82-43-98-24
74-54-53-20
65-00-36-28'''

llaves_originales = '''33-44-56-29
12-21-60-11
44-69-55-42
36-16-91-49
12-15-97-10
39-22-18-79
15-30-86-94
15-43-43-64
50-05-34-15
93-60-50-11
70-33-99-14
28-48-68-23
28-48-68-23
16-25-62-04
66-24-30-75
07-69-07-49'''

llaves_nuevos = '''42-82-96-81
33-63-65-52
65-61-28-63
86-36-38-37
86-00-80-21
82-79-75-47
60-97-92-93
62-46-79-20
39-01-91-47
75-17-74-58
45-43-12-68
86-14-43-38
62-96-18-17
56-56-06-08
34-58-83-62
11-16-41-61'''

##################################################################################
# Etapa 2: Obtención de controles y originales
##################################################################################

query = keys_parser().parse(controles_originales, query=True, db='EOD202009')
data, result = dl_sql(db='EOD202009').descarga(query=query, duplicates_level='interview__key')
data = data.loc[:, ['interview__key', 'tipo_muestra']]
data.to_csv(path.join('data', 'controles.csv'), index=False)

##################################################################################
# Etapa 3: Obtención de hogares nuevos y originales
##################################################################################

# Originales
query = keys_parser().parse(llaves_originales, query=True, db='EOD202009')
originales, result = dl_sql(db='EOD202009').descarga(query=query, duplicates_level='interview__key')
originales = originales.loc[:, ['interview__key', 'encuesta', 'idencuesta']]
originales.to_csv(path.join('data', 'originales.csv'), index=False)

# Nuevos
query = keys_parser().parse(llaves_nuevos, query=True, db='EOD202009')
nuevos, result = dl_sql(db='EOD202009').descarga(query=query, duplicates_level='interview__key')
nuevos = nuevos.loc[:, ['interview__key', 'encuesta', 'idencuesta']]
nuevos.to_csv(path.join('data', 'nuevos.csv'), index=False)
