from setup_helper import SetupHelper
from spark_session import get_spark

spark = get_spark("SBIT")
helper = SetupHelper(spark)
helper.setup()