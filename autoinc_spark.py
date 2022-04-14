from pyspark.shell import spark
from operator import add


def extract_make_key_value(x):
    return f'{x[1]}-{x[2]}'


file_location = "data.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table = df.createOrReplaceTempView('temp_table')

# select 'I' rows
df2 = df.where(df._c1 == 'I').select(df._c2, df._c3, df._c4, df._c5)

# select non 'I' rows
df3 = df.where(df._c1 != 'I')

# join the two dataframes togethre and select a new df where all the fields are populated
enhance_make = df3.join(df2, ["_c2"], 'left').select('_c1', 'temp_table._c3', 'temp_table._c5')

# extract the make and year as one string
make_kv = enhance_make.rdd.map(lambda x: extract_make_key_value(x))

# add the extracted make and year to a tuple and sum all tuples by key
make_kv_count = make_kv.map(lambda x: (x, 1)).reduceByKey(add)

print(*make_kv_count.collect(), sep='\n')
