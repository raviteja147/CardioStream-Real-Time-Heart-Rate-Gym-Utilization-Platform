from config import Config

class HistoryLoader():
    def __init__(self, spark):
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/raw"      
        self.test_data_dir = Conf.base_dir_data + "/test_data"
        self.db_name = Conf.db_name
        self.spark = spark
        
    def load_date_lookup(self):        
        print(f"Loading date_lookup table...", end='')        
        self.spark.sql(f"""INSERT OVERWRITE TABLE {self.db_name}.date_lookup 
                SELECT date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part 
                FROM json.`{self.test_data_dir}/6-date-lookup.json/`""")

        # # Read the JSON file
        # df = self.spark.read.json(f"{self.test_data_dir}/6-date-lookup.json/")
        
        # # Overwrite the Delta table
        # df.write.format("delta") \
        #     .mode("overwrite") \
        #     .option("overwriteSchema", "true") \
        #     .saveAsTable(f"{self.db_name}.date_lookup")
        #     #.save("spark-warehouse/sbit_db/date_lookup")
            
        print("Done")
        
    def load_history(self):
        import time
        start = int(time.time())
        print(f"\nStarting historical data load ...")
        self.load_date_lookup()
        print(f"Historical data load completed in {int(time.time()) - start} seconds")
        
    def assert_count(self, table_name, expected_count):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = self.spark.read.table(f"{self.db_name}.{table_name}").count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")        
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting historical data load validation...")
        self.assert_count(f"date_lookup", 365)
        print(f"Historical data load validation completed in {int(time.time()) - start} seconds")               
