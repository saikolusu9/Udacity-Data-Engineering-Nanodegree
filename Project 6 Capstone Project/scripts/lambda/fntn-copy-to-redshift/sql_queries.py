import os

# DROP TABLES
staging_collisions_table_drop = "DROP TABLE IF EXISTS staging_collisions"
staging_vehicles_table_drop = "DROP TABLE IF EXISTS staging_vehicles"
staging_persons_table_drop = "DROP TABLE IF EXISTS staging_persons"
staging_time_table_drop = "DROP TABLE IF EXISTS staging_time"
crashes_fact_table_drop = "DROP TABLE IF EXISTS fact_crashes"
collisions_table_drop = "DROP TABLE IF EXISTS dim_collisions"
vehicles_table_drop = "DROP TABLE IF EXISTS dim_vehicles"
persons_table_drop = "DROP TABLE IF EXISTS dim_persons"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
#create collisions staging table
staging_collisions_table_create= (""" CREATE TABLE IF NOT EXISTS staging_collisions 
(collision_id BIGINT, 
borough varchar,
street_name varchar,
contributing_factor_vehicle varchar, 
number_people_injured BIGINT, 
number_people_killed BIGINT
)
""")

#create vehicles staging table
staging_vehicles_table_create = (""" CREATE TABLE IF NOT EXISTS staging_vehicles 
(collision_id BIGINT,
vehicle_id BIGINT,
state_registration varchar,
vehicle_type varchar,
vehicle_make varchar,
vehicle_model varchar,
vehicle_year BIGINT,
driver_sex varchar,
point_of_impact varchar,
vehicle_damage varchar)
""")

#create persons staging table
staging_persons_table_create = (""" CREATE TABLE IF NOT EXISTS staging_persons 
(collision_id BIGINT,
person_id BIGINT,
person_type varchar,
person_injury varchar,
person_age BIGINT,
person_sex varchar)
""")

#create time staging table
staging_time_table_create = (""" CREATE TABLE IF NOT EXISTS staging_time 
(collision_id BIGINT,
crash_date_time timestamp PRIMARY KEY, 
year BIGINT NOT NULL,
month BIGINT NOT NULL, 
day BIGINT NOT NULL,
hour BIGINT NOT NULL, 
weekday BIGINT NOT NULL)
""")


#create song play table
fact_crashes_table_create = (""" CREATE TABLE IF NOT EXISTS fact_crashes
(crash_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
collision_id BIGINT REFERENCES dim_collisions, 
vehicle_id BIGINT REFERENCES dim_vehicles, 
person_id BIGINT REFERENCES dim_persons, 
crash_date_time timestamp REFERENCES dim_time
)
""")

#create users table
collision_table_create = (""" CREATE TABLE IF NOT EXISTS dim_collisions
(collision_id BIGINT PRIMARY KEY, 
borough varchar,
street_name varchar,
contributing_factor_vehicle varchar, 
number_people_injured BIGINT, 
number_people_killed BIGINT
)
""")

#create songs table
vehicles_table_create = (""" CREATE TABLE IF NOT EXISTS dim_vehicles
(collision_id BIGINT,
vehicle_id BIGINT PRIMARY KEY,
state_registration varchar,
vehicle_type varchar,
vehicle_make varchar,
vehicle_model varchar,
vehicle_year BIGINT,
driver_sex varchar,
point_of_impact varchar,
vehicle_damage varchar)
""")

#create artists table
persons_table_create = ("""CREATE TABLE IF NOT EXISTS dim_persons
(collision_id BIGINT,
person_id BIGINT PRIMARY KEY,
person_type varchar,
person_injury varchar,
person_age BIGINT,
person_sex varchar)
""")

#create time table
time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time
(collision_id BIGINT,
crash_date_time timestamp PRIMARY KEY, 
year BIGINT NOT NULL,
month BIGINT NOT NULL, 
day BIGINT NOT NULL,
hour BIGINT NOT NULL, 
weekday BIGINT NOT NULL)
""")

# STAGING TABLES

#copy into collisions staging table
staging_collisions_copy = ("""copy staging_collisions 
from '{}'
iam_role '{}'
FORMAT AS PARQUET;
""").format('/'.join(["s3:/",os.environ['bucket'], os.environ['input_folder'], os.environ['crashes_data'],""]), os.environ['arn'])

#copy into vehicles staging table
staging_vehicles_copy = ("""copy staging_vehicles 
from '{}'
iam_role '{}'
FORMAT AS PARQUET;
""").format('/'.join(["s3:/",os.environ['bucket'], os.environ['input_folder'], os.environ['vehicles_data'],""]), os.environ['arn'])

#copy into songs staging table
staging_persons_copy = ("""copy staging_persons 
from '{}'
iam_role '{}'
FORMAT AS PARQUET;
""").format('/'.join(["s3:/",os.environ['bucket'],os.environ['input_folder'], os.environ['persons_data'],""]), os.environ['arn'])

#copy into songs staging table
staging_time_copy = ("""copy staging_time 
from '{}'
iam_role '{}'
FORMAT AS PARQUET;
""").format('/'.join(["s3:/",os.environ['bucket'],os.environ['input_folder'], os.environ['time_data'],""]), os.environ['arn'])


# FINAL TABLES

#insert into songplay table
collisions_table_insert = (""" INSERT INTO dim_collisions
Select * from staging_collisions
where
collision_id not in (
Select collision_id from dim_collisions
)
""")

#insert into users table
vehicles_table_insert = (""" INSERT INTO dim_vehicles
Select * from staging_vehicles
where 
vehicle_id not in (
Select vehicle_id from dim_vehicles
)
""")

#insert into songs table
persons_table_insert = (""" INSERT INTO dim_persons
Select * from staging_persons
where person_id not in 
(Select person_id from dim_persons)
""")

#insert into artists table
time_table_insert = (""" INSERT INTO dim_time
Select * from staging_time
where crash_date_time not in 
(Select crash_date_time from dim_time) 
""")

#insert into time table
fact_crashes_insert = (""" INSERT INTO fact_crashes 
(collision_id, vehicle_id, person_id, crash_date_time)
Select distinct 
a.COLLISION_ID, 
b.VEHICLE_ID, 
c.PERSON_ID, 
d.CRASH_DATE_TIME
from staging_collisions a 
join staging_vehicles b
on a.collision_id = b.collision_id
join staging_persons c
on a.collision_id = c.collision_id
join staging_time d
on a.collision_id = d.collision_id
""")


# QUERY LISTS
create_table_queries = [staging_collisions_table_create, staging_vehicles_table_create, staging_persons_table_create, staging_time_table_create, collision_table_create, vehicles_table_create, persons_table_create, time_table_create,fact_crashes_table_create]
drop_table_queries = [staging_collisions_table_drop ,staging_vehicles_table_drop ,staging_persons_table_drop ,staging_time_table_drop,crashes_fact_table_drop ,collisions_table_drop ,vehicles_table_drop ,persons_table_drop ,time_table_drop ]
copy_table_queries = [staging_collisions_copy, staging_vehicles_copy, staging_persons_copy, staging_time_copy]
insert_table_queries = [collisions_table_insert, vehicles_table_insert, persons_table_insert, time_table_insert,fact_crashes_insert]

