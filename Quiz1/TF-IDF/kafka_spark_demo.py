import findspark
# findspark.init()
findspark.init('C:\spark-2.4.7-bin-hadoop2.6')

# import org.apache.spark.streaming.kafka._
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark
import math
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

############################ MAIN ###################################
if __name__=="__main__":
    sc = SparkContext(appName="Kafka Spark Demo")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,60)
    
    # msg = KafkaUtils.createDirectStream(ssc, topics=["testtopic"],kafkaParams={"metadata.broker.list":"localhost:9092"})
    
    # words = msg.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))
    # wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
    # wordcount.pprint()

    ###################### TF-IDF ###################################
    # data=[(1,'i love dogs'),(2,"i hate dogs and knitting"),(3,"knitting is my hobby and my passion")]

    # sc =SparkContext()
    lines=sc.parallelize(data)

    # lines.collect()

    map1=lines.flatMap(lambda x: [((x[0],i),1) for i in x[1].split()])

    reduce=map1.reduceByKey(lambda x,y:x+y)

    tf=reduce.map(lambda x: (x[0][1],(x[0][0],x[1])))

    map3=reduce.map(lambda x: (x[0][1],(x[0][0],x[1],1)))

    # map3.collect()

    map4=map3.map(lambda x:(x[0],x[1][2]))

    # map4.collect()

    reduce2=map4.reduceByKey(lambda x,y:x+y)

    # reduce2.collect()

    idf=reduce2.map(lambda x: (x[0],math.log10(len(data)/x[1])))

    # idf.collect()

    idf=reduce2.map(lambda x: (x[0],math.log10(len(data)/x[1])))

    # idf.collect()

    rdd=tf.join(idf)

    rdd=rdd.map(lambda x: (x[1][0][0],(x[0],x[1][0][1],x[1][1],x[1][0][1]*x[1][1]))).sortByKey()

    rdd=rdd.map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3]))

    rdd.collect()

    rdd.pprint()
    ###################### TF-IDF ###################################

    ssc.start()
    ssc.awaitTermination()
