from audioop import add
from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "CarSales")
raw_rdd = sc.textFile("data.csv")


def extract_vin_key_value(row):
    return row[2], (row[1], row[3], row[5])


# def populate_make(x):
#     if x[0] and x[1]:
#         return x


def extract_make_key_value(x):
    return f'{x[1]}-{x[2]}'


vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

enhance_make = vin_kv.groupByKey().flatMap(lambda kv: kv[1]).filter(lambda x: x[0] == 'I')

make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

make_kv_count = make_kv.map(lambda x: (x, 1)).reduceByKey(add)

print(*make_kv_count.collect(), sep='\n')
