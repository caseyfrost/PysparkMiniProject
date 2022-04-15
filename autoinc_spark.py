from operator import add
from pyspark.sql import SparkSession


def extract_make_key_value(x):
    return f'{x[1]}-{x[2]}'


file_location = "data.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

spark = SparkSession.builder.appName('CarData').getOrCreate()

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# select 'I' rows
df2 = df.where(df._c1 == 'I').select('_c2', '_c3', '_c4', '_c5')

# select non 'I' rows
df3 = df.where(df._c1 != 'I')

enhance_make = df3.alias('df3').join(df2.alias("df2"), ["_c2"], 'left').select('_c1', 'df2._c3', 'df2._c5')

make_kv = enhance_make.rdd.map(lambda x: extract_make_key_value(x))

make_kv_count = make_kv.map(lambda x: (x, 1)).reduceByKey(add)

print(*make_kv_count.collect(), sep='\n')
