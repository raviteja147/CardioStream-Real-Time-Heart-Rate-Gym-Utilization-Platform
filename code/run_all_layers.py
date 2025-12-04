from bronze import Bronze
from silver import Silver
from gold import Gold

class RunAllLayers:
    def __init__(self, spark):
        self.spark = spark
        self.BZ = Bronze(self.spark)
        self.SL = Silver(self.spark)
        self.GL = Gold(self.spark)

    def execute(self, once = True, processing_time = "5 seconds"):
        self.BZ.consume(once, processing_time)
        self.SL.upsert(once, processing_time)
        self.GL.upsert(once, processing_time)

    def validate_first_ingestion(self):
        self.BZ.validate(1)
        self.SL.validate(1)
        self.GL.validate(1)

    def validate_second_ingestion(self):
        self.BZ.validate(2)
        self.SL.validate(2)
        self.GL.validate(2)
        

        
        
        
        