import pymongo
from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase
import psycopg2
import redis
from pymongo import MongoClient

# config
# ES
index_name = "mirea.public.materials"
es = Elasticsearch("http://localhost:9200")
# es = Elasticsearch("http://25.8.8.1:9200")

# Neo
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


#Mongo
client = pymongo.MongoClient("mongodb://root:2517Pass!Part@localhost:27017/?authMechanism=DEFAULT")  # Замените на ваш URL
db = client["db_mirea"]  # Замените на название вашей базы данных
collection = db["smelkin"]  # Замените на название вашей коллекции
# CONNECTION_STRING = "mongodb://root:2517Pass!Part@25.8.8.1:27017/?authMechanism=DEFAULT"

# Значения для запроса
semestr_for_query = 1 # 1 или 2
year_for_query = 2023 # 2023 или 2024

def get_dates(semestr,year):
    if semestr == 1:
            year = str(year)
            return (str(year + "-09-01"), str(year + "-12-29"))
    elif semestr == 2:
        year = str(year + 1)
        return (str(year + "-01-09"), str(year + "-05-31"))

detes_period = get_dates(semestr_for_query,year_for_query)


#1 названия курсов из NEO с требованиями к тех.средствам +
discip_names = []
discip_names =  graphDB_Driver.session().run("MATCH (d:Disciplines) where d.technical<>'' return d.name",yaer=str(year_for_query), semestr=str(semestr_for_query)).value()
print(discip_names)


#id занятий которые проводятся в x семестре y года +
lec_id=[]
with graphDB_Driver.session() as neo_session:
        lec_id.append(neo_session.run("MATCH (d:Disciplines)--(l:Lecture)--(tt:TimeTable) WHERE d.technical<>'' and tt.lecture_id=l.id RETURN  l.id",start=str(detes_period[0]),end=str(detes_period[1])).data())

lec_ids=[]
for i in range(len(lec_id[0])):
        lec_ids.append((lec_id[0][i]['l.id']))
print(lec_ids)
#TODO:объединение в один запрос без цикла ниже

#вывод дисциплины и слушателей в периоде
array = []

with graphDB_Driver.session() as neo_session:
    for disc in discip_names:
        count_st=(neo_session.run("MATCH (d:Disciplines)--(l:Lecture)--(tt:TimeTable)--(gr:Group)--(st:Student) WHERE d.name = $name and l.id in $lec RETURN count (distinct st)",name=disc,lec=(lec_ids)).data())
        count = (count_st[0]['count (distinct st)'])
        array.append((disc,count))
print(array)

# #формирование отчёта
print("\nОтчет:")
for line in array:
    def find_discipline(collection, discipline_name):
        pipeline = [
            {"$unwind": "$kafedras"},
            {"$unwind": "$kafedras.specialnosts"},
            {"$unwind": "$kafedras.specialnosts.disciplines"},
            {"$match": {"kafedras.specialnosts.disciplines.name": discipline_name}}
        ]
        discipline = list(collection.aggregate(pipeline))
        if discipline:
            institute_name = discipline[0]["name"]
            kafedra = discipline[0]["kafedras"]
            specialnost = kafedra["specialnosts"]
            discipline = specialnost["disciplines"]
            return kafedra, specialnost, discipline, institute_name
        return None, None, None, None


    # Поиск и вывод информации о дисциплине
    kafedra, specialnost, discipline, institute_name = find_discipline(collection, line[0])

    if discipline:
        print("\nНайдена дисциплина:",  line[0])
        print("Слушателей: "+str(line[1])+" за "+str(semestr_for_query)+" семестр "+str(year_for_query)+" года")
        print("Подробнее:")
        print("\tинститут:", institute_name)
        print("\tКафедра:", kafedra["name"])
        print("\tСпециальность:", specialnost["name"])
        print("\tТехнические требования:", discipline["technical"])
        print("\tОписание курса:", discipline["description"])
    else:
        print("Дисциплина",  line[0], "не найдена.")