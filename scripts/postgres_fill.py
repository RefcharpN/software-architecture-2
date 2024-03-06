import random
import time
import psycopg2

database_name = "mirea"
user_name = "postgres"
password = "2517Pass!Part"
host_ip = "25.8.8.1"
host_port = "5432"

# my_con = psycopg2.connect(
#     database=database_name,
#     user=user_name,
#     password=password,
#     host=host_ip,
#     port=host_port
# )
#
# my_con.autocommit = True
# cursor = my_con.cursor()

# database_name = "mirea"

my_db_con = psycopg2.connect(
    database=database_name,
    user=user_name,
    password=password,
    host=host_ip,
    port=host_port
)
# Дисциплины
create_disciplines = """
create table disciplines
(
    id              serial not null
        constraint disciplines_pk
            primary key,
    title           text   not null,
    type_of_control text   not null,
    description     text   not null
);
"""

# Lecture связана с Disciplines полем Discipline
create_lecture = """
create table lecture
(
    id            serial not null
        constraint lecture_pk
            primary key,
    type          text   not null,
    title         text   not null,
    discipline_id integer
        constraint discipline_id
            references disciplines
);
"""
# Группы
create_groups = """
create table groups
(
	id serial not null
		constraint groups_pk
			primary key,
	title text not null,
	course int not null,
	spec_id text not null
        constraint spec_id
            references spec
);
"""
# Студенты связаны с группами
create_students = """
create table students
(
    id       serial not null
        constraint students_pk
            primary key,
    code     text   not null,
    fio      text   not null,
    group_id integer
        constraint group_id
            references groups
);
"""

# План обучения
create_plan = """
create table plan
(
    id            serial  not null
        constraint plan_pk
            primary key,
    semestr       integer not null,
    year          integer not null,
    hours         integer not null,
    spec          text not null,
    discipline_id integer
        constraint discipline_id
            references disciplines
);
"""
# Timestamp

create_timestamps = """
create table timestamps
(
    id           serial  not null
        constraint timestamps_pk
            primary key,
    logdate      date    not null,
    cabinet      integer not null,
    week         integer not null,
    hours        integer not null,
    teachers_fio text    not null,
    lecture_id   integer not null
        constraint lecture_id
            references lecture,
    group_id     integer not null
        constraint group_id
            references groups
);
"""
create_cafedra = """
create table cafedra
(
    id           text  not null
        constraint cafedra_pk
            primary key,
    title text    not null,
    institute_id   text not null
        constraint institute_id
            references institute
);
"""

create_institute = """
create table institute
(
    id           text  not null
        constraint institute_pk
            primary key,
    title text    not null
);
"""

create_spec = """
create table spec
(
    id           text  not null
        constraint spec_pk
            primary key,
    title text    not null,
    cafedra_id   text not null
        constraint cafedra_id
            references cafedra
);
"""

# Посещения связаны с студентами и таймстэмпом
create_visits = """
create table visits
(
    id           serial  not null
        constraint visits_pk
            primary key,
    student_id   integer not null
        constraint student_id
            references students,
    was          boolean not null,
    timestamp_id integer
        constraint timestamp_id
            references timestamps
);
"""
alter_table_cafedra = "ALTER TABLE cafedra REPLICA IDENTITY FULL"
alter_table_disciplines = "ALTER TABLE disciplines REPLICA IDENTITY FULL"
alter_table_groups = "ALTER TABLE groups REPLICA IDENTITY FULL"
alter_table_institute = "ALTER TABLE institute REPLICA IDENTITY FULL"
alter_table_lecture = "ALTER TABLE lecture REPLICA IDENTITY FULL"
alter_table_plan = "ALTER TABLE plan REPLICA IDENTITY FULL"
alter_table_spec = "ALTER TABLE spec REPLICA IDENTITY FULL"
alter_table_students = "ALTER TABLE students REPLICA IDENTITY FULL"
alter_table_timestamps = "ALTER TABLE timestamps REPLICA IDENTITY FULL"
alter_table_visits = "ALTER TABLE visits REPLICA IDENTITY FULL"
my_db_con.autocommit = True
cursor = my_db_con.cursor()
cursor.execute(create_institute)
cursor.execute(create_cafedra)
cursor.execute(create_spec)
cursor.execute(create_disciplines)
cursor.execute(create_lecture)
cursor.execute(create_groups)
cursor.execute(create_students)
cursor.execute(create_plan)
cursor.execute(create_timestamps)
cursor.execute(create_visits)
cursor.execute(alter_table_cafedra)
cursor.execute(alter_table_disciplines)
cursor.execute(alter_table_groups)
cursor.execute(alter_table_institute)
cursor.execute(alter_table_lecture)
cursor.execute(alter_table_plan)
cursor.execute(alter_table_spec)
cursor.execute(alter_table_students)
cursor.execute(alter_table_timestamps)
cursor.execute(alter_table_visits)

insitute_insert = "INSERT INTO institute(id, title) VALUES ('ICDT', 'Institute of Cybersecurity " \
                  "and Digital Technology'), ('IE', 'Institute of Economy'), ('HI', 'Heisenbergs Institute')"
cafedra_insert = "INSERT INTO cafedra(id, title, institute_id) VALUES ('KB3', 'Security of software solutions', 'ICDT')," \
                 "('KE', 'Economy', 'IE'), ('DCT', 'Chemistry and technologies of elastomer processing', 'HI')," \
                 "('KB14','Digital data processing technologies', 'ICDT')"
spec_insert = "INSERT INTO spec(id, title, cafedra_id) VALUES('Tech', 'Statistics', 'KB14')," \
              "('Economy', 'Economy', 'KE'), ('Chemistry', 'Chemistry', 'DCT')"

disciplines_insert = "INSERT INTO disciplines (id, title, type_of_control, description) VALUES (1, 'Mat analisys', " \
                     "'Lection', 'Many numbers'), (2, 'History', 'Lection', 'Many words')" \
                     ", (3, 'Programming', 'Lection', 'Many code')"

groups_insert = "INSERT INTO groups (id, title, course, spec_id) VALUES (1, 'BSBO-02-19', 4, 'Tech'), (2, 'BEBO-01-19', 4, 'Economy'), (3, 'BCBO-03-20', 3, 'Chemistry'), " \
                "(4, 'BSBO-03-21', 2, 'Tech')"

lecture_insert = "INSERT INTO public.lecture (id, type, title, discipline_id) VALUES (1, 'Lection', 'Limits', 1), (2, 'Lection', 'Russian history', 2)," \
                 "(3, 'Lection', 'Metric space', 1), (4, 'Practice', 'Limits', 1), (5, 'Practice', 'Russian history', 2), (6, 'Practice', 'Metric space', 1)," \
                 "(7, 'Lection', 'Arrays', 3), (8, 'Lection', 'Europe history', 2), (9, 'Practice', 'Europe history', 2), " \
                 "(10, 'Practice', 'Arrays', 3), (11, 'Lection', 'Boolean expressions', 3), (12, 'Practice', 'Boolean expressions', 3)"

plan_insert = "INSERT INTO public.plan (id, semestr, year, hours, spec, discipline_id) VALUES (1, 2, 2022, 144, 'Tech', 1), (2, 2, 2021, 72, 'Tech', 2)," \
              "(3, 2, 2021, 360, 'Tech', 1), (4, 2, 2021, 360, 'Economy', 1), (5, 2, 2022, 144, 'Economy', 2), (6, 2, 2023, 144, 'Economy', 3), (7, 2, 2023, 72, 'Chemistry', 1)," \
              "(8, 2, 2022, 72, 'Chemistry', 2), (9, 2, 2023, 360, 'Chemistry', 3)"

students_insert = "INSERT INTO public.students (id, code, fio, group_id) VALUES (1, '19B0123', 'Smith DY', 1), (2, '19B0124', 'Johnson AA', 1), (3, '19B0125', 'Brown DI', 1), " \
                  "(4, '19B0126', 'Taylor AI', 1), (5, '19B0127', 'Davis DI', 1), (6, '19B0434', 'Wilson AG', 2), (7, '19B0435', 'Robinson FR', 2), " \
                  "(8, '19B0436', 'Martinez EP', 2), (9, '19B0437', 'Garcia SD', 2), (10, '19B0438', 'Ahmed AA', 2), (11, '19B0534', 'Adams AV', 3), (12, '19B0535', 'Evans EA', 3), " \
                  "(13, '19B0536', 'Parker AS', 3), (14, '19B0537', 'Baker AH', 3), (15, '19B0538', 'Gonzalez AS', 3), (16, '19B0777', 'Mitchell LS', 4), (17, '19B0633', 'Perez KL', 4), " \
                  "(18, '19B0322', 'Wright EV', 4), (19, '19B0228', 'Hernandez HA', 4), (20, '19B0499', 'Sanchez SA', 4)"

cursor.execute(insitute_insert)
time.sleep(10)
cursor.execute(cafedra_insert)
time.sleep(10)
cursor.execute(spec_insert)
time.sleep(10)
cursor.execute(disciplines_insert)
time.sleep(10)
cursor.execute(groups_insert)
time.sleep(10)
cursor.execute(lecture_insert)
time.sleep(10)
cursor.execute(plan_insert)
time.sleep(10)
cursor.execute(students_insert)
time.sleep(10)
logdate_year_list = [2021,2022,2023]
teachers_list = ["Ermakova AU", "Kashkin EV", "Rusakov AM", "Serov BA", "Lesko CA", "Shpunt CC",
                 "Beklemishev CA", "Koryagin CB", "Latypov IT", "Kriulin AA",
                 "Sadykov IS", "Borisov AA", "Gorin DS", "Filatov BB"]

for i in range(1,201):
    query_str = "(" + str(i) + ", '" + str(logdate_year_list[random.randint(0,(len(logdate_year_list)-1))]) + "-" + str(random.randint(1,12)) + "-" + str(random.randint(1,28)) + "', " + str(random.randint(100,399)) + ", " + str(random.randint(1,16)) + ", " + str(random.randint(1,6)) + ", '" + str(teachers_list[random.randint(0,len(teachers_list)-1)]) + "', " + str(random.randint(1,12)) + ", " + str(random.randint(1,4)) + ")"
    timestamps_insert = "INSERT INTO public.timestamps (id, logdate, cabinet, week, hours, teachers_fio, lecture_id, group_id) VALUES " + query_str
    cursor.execute(timestamps_insert)

time.sleep(10)
check_list= ["true", "false"]
for i in range(1,201):
    query_str = "(" + str(i) + ", " + str(random.randint(1,20)) + ", " + str(check_list[random.randint(0,1)]) + ", " + str(random.randint(1,20)) + ")"
    visits_insert = "INSERT INTO public.visits (id, student_id, was, timestamp_id) VALUES "  + query_str
    cursor.execute(visits_insert)

print("pg filled")
