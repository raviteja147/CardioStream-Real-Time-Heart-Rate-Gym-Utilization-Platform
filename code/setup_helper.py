from config import Config
import shutil
import os

class SetupHelper():   
    def __init__(self, spark):
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/raw"
        self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"        
        self.db_name = Conf.db_name
        self.initialized = False
        self.spark = spark
        
    def create_db(self):
        #print(f"Creating the database {self.db_name}...", end='')
        self.spark.sql(f"""
                    CREATE DATABASE IF NOT EXISTS {self.db_name}
                    LOCATION '{self.db_name}'
                    """)
        
        self.initialized = True
        #print("Done")

    def create_raw_folders(self):
        folders = ["data_zone/raw/gym_logins_bz", "data_zone/raw/kafka_multiplex_bz", "data_zone/raw/registered_users_bz"]

        for folder in folders:
            os.makedirs(folder, exist_ok=True)
            #print(f"Created: {folder}")
        
    def create_registered_users_bz(self):
        if(self.initialized):
            #print(f"Creating registered_users_bz table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.registered_users_bz(
                    user_id long,
                    device_id long, 
                    mac_address string, 
                    registration_timestamp double,
                    load_time timestamp,
                    source_file string                    
                    ) USING DELTA
                    LOCATION '{self.db_name}/registered_users_bz'
                  """) 
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    
    def create_gym_logins_bz(self):
        if(self.initialized):
            #print(f"Creating gym_logins_bz table...", end='')
            self.spark.sql(f"""CREATE OR REPLACE TABLE {self.db_name}.gym_logins_bz(
                    mac_address string,
                    gym bigint,
                    login double,                      
                    logout double,                    
                    load_time timestamp,
                    source_file string
                    ) USING DELTA
                    LOCATION '{self.db_name}/gym_logins_bz'
                  """) 
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_kafka_multiplex_bz(self):
        if(self.initialized):
            #print(f"Creating kafka_multiplex_bz table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.kafka_multiplex_bz(
                  key string, 
                  value string, 
                  topic string, 
                  partition bigint, 
                  offset bigint, 
                  timestamp bigint,                  
                  date date, 
                  week_part string,                  
                  load_time timestamp,
                  source_file string) USING DELTA
                  PARTITIONED BY (topic, week_part)
                  LOCATION '{self.db_name}/kafka_multiplex_bz'
                  """) 
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")       
    
            
    def create_users(self):
        if(self.initialized):
            #print(f"Creating users table...", end='')
            self.spark.sql(f"""CREATE OR REPLACE TABLE {self.db_name}.users(
                    user_id bigint, 
                    device_id bigint, 
                    mac_address string,
                    registration_timestamp timestamp
                    ) USING DELTA
                    LOCATION '{self.db_name}/users'
                  """)  
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")            
    
    def create_gym_logs(self):
        if(self.initialized):
            #print(f"Creating gym_logs table...", end='')
            self.spark.sql(f"""CREATE OR REPLACE TABLE {self.db_name}.gym_logs(
                    mac_address string,
                    gym bigint,
                    login timestamp,                      
                    logout timestamp
                    ) USING DELTA
                    LOCATION '{self.db_name}/gym_logs'
                  """) 
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_user_profile(self):
        if(self.initialized):
            #print(f"Creating user_profile table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.user_profile(
                    user_id bigint, 
                    dob DATE, 
                    sex STRING, 
                    gender STRING, 
                    first_name STRING, 
                    last_name STRING, 
                    street_address STRING, 
                    city STRING, 
                    state STRING, 
                    zip INT, 
                    updated TIMESTAMP) USING DELTA 
                    LOCATION '{self.db_name}/user_profile'
                  """) 
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_heart_rate(self):
        if(self.initialized):
            #print(f"Creating heart_rate table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.heart_rate(
                    device_id LONG, 
                    time TIMESTAMP, 
                    heartrate DOUBLE, 
                    valid BOOLEAN) USING DELTA
                    LOCATION '{self.db_name}/heart_rate'
                  """)
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

            
    def create_user_bins(self):
        if(self.initialized):
            #print(f"Creating user_bins table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.user_bins(
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING) USING DELTA 
                    LOCATION '{self.db_name}/user_bins'
                  """)  
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workouts(self):
        if(self.initialized):
            #print(f"Creating workouts table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.workouts(
                    user_id INT, 
                    workout_id INT, 
                    time TIMESTAMP, 
                    action STRING, 
                    session_id INT) USING DELTA 
                    LOCATION '{self.db_name}/workouts'
                  """)  
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_completed_workouts(self):
        if(self.initialized):
            #print(f"Creating completed_workouts table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.completed_workouts(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT, 
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP) USING DELTA 
                    LOCATION '{self.db_name}/completed_workouts'
                  """)  
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workout_bpm(self):
        if(self.initialized):
            #print(f"Creating workout_bpm table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.workout_bpm(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT,
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP,
                    time TIMESTAMP, 
                    heartrate DOUBLE) USING DELTA 
                    LOCATION '{self.db_name}/workout_bpm'
                  """)  
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_date_lookup(self):
        if(self.initialized):
            #print(f"Creating date_lookup table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.date_lookup(
                    date date, 
                    week int, 
                    year int, 
                    month int, 
                    dayofweek int, 
                    dayofmonth int, 
                    dayofyear int, 
                    week_part string) USING DELTA 
                    LOCATION '{self.db_name}/date_lookup'
                  """)  
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_workout_bpm_summary(self):
        if(self.initialized):
            #print(f"Creating workout_bpm_summary table...", end='')
            self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.workout_bpm_summary(
                    workout_id INT, 
                    session_id INT, 
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING, 
                    min_bpm DOUBLE, 
                    avg_bpm DOUBLE, 
                    max_bpm DOUBLE, 
                    num_recordings BIGINT) USING DELTA 
                    LOCATION '{self.db_name}/workout_bpm_summary'
                  """)
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_gym_summary(self):
        if(self.initialized):
            #print(f"Creating gym_summar gold view...", end='')
            self.spark.sql(f"""CREATE OR REPLACE VIEW {self.db_name}.gym_summary AS
                            SELECT to_date(CAST(login AS timestamp)) AS date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((CAST(logout AS long) - CAST(login AS long))/60, 2) AS minutes_in_gym,
                           round((CAST(end_time AS long) - CAST(start_time AS long))/60, 2) AS minutes_exercising
                            FROM {self.db_name}.gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM {self.db_name}.completed_workouts w INNER JOIN {self.db_name}.users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
                        """)
            #print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def setup(self):
        import time
        start = int(time.time())
        #print(f"\nStarting setup ...")
        self.create_db() 
        self.create_raw_folders()
        self.create_registered_users_bz()
        self.create_gym_logins_bz() 
        self.create_kafka_multiplex_bz()        
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()  
        self.create_gym_summary()
        #print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, table_name):
        assert self.spark.sql(f"SHOW TABLES IN {self.db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert self.spark.sql(f"SHOW DATABASES") \
                    .filter(f"namespace == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.db_name}' is missing"
        print(f"Found database {self.db_name}: Success")
        self.assert_table("registered_users_bz")   
        self.assert_table("gym_logins_bz")        
        self.assert_table("kafka_multiplex_bz")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("user_bins")
        self.assert_table("date_lookup")
        self.assert_table("workout_bpm_summary") 
        self.assert_table("gym_summary") 
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self):
        if self.spark.sql(f"SHOW DATABASES").filter(f"namespace == '{self.db_name}'").count() == 1:
            print(f"Dropping the database{self.db_name}...", end='')
            self.spark.sql(f"DROP DATABASE {self.db_name} CASCADE")
            print("Done")

        folders_to_delete = [
                self.landing_zone,
                self.checkpoint_base,
                "spark-warehouse/"
            ]

        for folder in folders_to_delete:
            if os.path.exists(folder):
                shutil.rmtree(folder)
                print(f"{folder} deleted successfully")
