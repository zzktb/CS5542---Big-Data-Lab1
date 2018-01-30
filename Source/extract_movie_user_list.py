from pyspark import SparkContext

sc = SparkContext("local")
data_path = "D:\\umkc\\2018Spring\\Big_data_analytics\\lecture_03\\ml-100k\\u.data"

# Read data
data = sc.textFile(data_path)

# Split
user_data = data.map(lambda x: x.split("\t"))

# Group wrt the user ID
user_item = user_data.map(lambda x: (int(x[0]), int(x[1]))).groupByKey()

# Find those who rated more than 25 times
target = user_item.map(lambda x: (x[0], len(x[1]))).filter(lambda x: x[1] > 25)

# save output to file
output = sc.parallelize(target.collect())
output.saveAsTextFile("results")

# print summary
print("Number of all users: " + str(user_item.count()))
print("Number of target users: " + str(target.count()))

sc.stop()

