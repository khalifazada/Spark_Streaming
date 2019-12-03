from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json

if __name__ == "__main__":

	# create session & contexts
	sc = SparkContext("local[*]", "twitterStreamContent")
	ssc = StreamingContext(sc, 3)
	
	ss = SparkSession.builder \
			.appName(sc.appName) \
			.config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
			.config("hive.metastore.uris", "thrift://localhost:9083") \
			.enableHiveSupport() \
			.getOrCreate()

	# set Kafka DStream
	kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", \
												"spark-streaming", \
												{"twitter" : 1})
	
	# parse tweets
	parsed = kafkaStream.map(lambda x: json.loads(x[1])) \
						.filter(lambda x: x.get("lang") == "en") \
						.map(lambda x: (x.get("id"), x.get("text")))

	# custom function - process parsed tweets as df & append to Hive
	def process(rdd):
		if not rdd.isEmpty():
			global ss
			df = ss.createDataFrame(rdd, schema=["id", "text"])
			df.show()
			df.write.saveAsTable(name="tweets_db.tweets_tbl", format="hive", mode="append")

	# write parsed tweets to Hive
	parsed.foreachRDD(process)

	# start streaming context
	ssc.start()

	# stop streaming context (auto)
	ssc.awaitTermination()
