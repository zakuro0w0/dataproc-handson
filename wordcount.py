from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()
text_file = spark.read.text("gs://naritake-dataproc-bucket/hello-world.txt")
words = text_file.selectExpr("split(value, ',') as words")
word_counts = words.rdd.flatMap(lambda x: x).countByValue()

for word, count in word_counts.items():
    print(f"{word}: {count}")

spark.stop()
