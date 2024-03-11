import json

from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase
import psycopg2
import redis

# elasticsearch
index_name = "mirea.public.materials"
es = Elasticsearch("http://localhost:9200")
# es = Elasticsearch("http://25.8.8.1:9200")


# Neo4j
uri = "bolt://localhost:7687"
# uri = "bolt://25.8.8.1:7687"
userName = "neo4j"
password = "2517Pass!Part"

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

#postgresql
database_name = "mirea"
user_name = "postgres"
password = "2517Pass!Part"
host_ip = "localhost"
# host_ip = "25.8.8.1"
host_port ="5432"

connection = psycopg2.connect(
            database = database_name,
            user = user_name,
            password = password,
            host = host_ip,
            port = host_port
)

connection.autocommit = True
cursor = connection.cursor()

#Redis
redis = redis.Redis(host= 'localhost', port= '6379', db=0)
# redis = redis.Redis(host= '25.8.8.1', port= '6379', db=0)


# Значения для запроса
student_limit = 10
phrase = 'разъясню'
begin_date = '2023-09-01'
end_date = '2024-05-29'

query_body = {
    #"size":100,
    "query": {
        "query_string": {
            "query":  "*%s*" % (phrase),
			"default_field": "after.material"
        }
    }
}

#1
# id лекцции из материалов где фраза
materials_with_phrase = es.search(index=index_name, body=query_body)

print("выборка из elastic")
print(materials_with_phrase['hits']['hits'])

materials_with_phrase_ids = []
for lection_id in materials_with_phrase['hits']['hits']:
	materials_with_phrase_ids.append(lection_id['_source']['after']['lecture_id'])
print("выборка из elastic материалы с фразой")
print(materials_with_phrase_ids)

#2 студенты, посещающие занятия
students = []
# print("выборка  из neo")
for lection_id in materials_with_phrase_ids:
    for student in graphDB_Driver.session().run("MATCH (l:Lecture{id:$id})--(d:Disciplines)--(s:Specialnost)--(g:Group)--(st:Student) RETURN  st", id=lection_id).data():
        students.append(student['st']['id_stud_code'])
        # print(student)
print("выборка из neo студенты у которых лекции")
print(students)

#3 статистика посещаемости
querry_pattern = '''SELECT (CAST(SUM(CASE WHEN visited THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) AS percent, student_id
FROM visits
WHERE student_id in (%s)
AND date_visit between '%s' AND '%s'
GROUP BY student_id
ORDER BY percent
LIMIT '%s'
'''
cursor.execute(querry_pattern % (','.join(map(str,students)), str(begin_date), str(end_date), str(student_limit)))
result=cursor.fetchall()
# print("выборка из pg")
# print(result)

#4 формирование отчёта
print("\nОтчет:")
for i in result:
    print("№ "+str(i[1])+"\t"+json.loads(redis.get(i[1]).decode())['fio']+"\t"+str(int(i[0]*100))+"%\t"+ "c "+ str(begin_date)+"\tпо "+str(end_date)+" - "+phrase)
