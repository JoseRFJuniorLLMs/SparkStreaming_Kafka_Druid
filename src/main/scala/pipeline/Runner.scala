package pipeline

import java.io.FileReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import pipeline.config.InputConfig
import pipeline.spark.SparkStreamingTasks

object Runner {

  def main(args: Array[String]): Unit = {
    
    if(args.length < 1) {
      println("Usage : Runner CONFIG_FILE\n")
      println("Sample Config File : ")
      val config_yaml = """
			|zookeeper: slcmsgkzookeeper8714a.slc.paypal.com:2181,slcmsgkzookeeper8712a.slc.paypal.com:2181"
			|kafka: 
  		|	topic: "fpti.platform.enrch"
  		|	broker: "slckafka1a.slc.paypal.com:9092"
			|spark:
  		|	appname: "fpti-events-streaming-spark"
  		|	master: "local[*]"
  		|	durationS: 10
  		|	checkpoint: "/tmp/fpti-spark/realtime/checkpoints"
  		|	kafka:
    	|		group: "fpti-events-streaming-kafka"
    	|		properties:
      |			- property: "zookeeper.connection.timeout.ms"
      |				value: "1000"
      |			- property: "refresh.leader.backoff.ms"
      |				value: "500"
			|druid:
  		|	zookeeper: "lvsdtplatform09.lvs.paypal.com:2181"
  		|	indexer: "druid/overlord"
  		|	discovery: "/druid/discovery"
  		|	datasource: "druid_ingest"
  		|	curator-retry:
    	|		baseSleepMs: 100
    	|		maxSleepMs: 3000
    	|		retries: 5
  		|	tuning:
    	|		segmentGranularity: "HOUR"
    	|		windowPeriod: "PT10M"
    	|		partitions: 1
    	|		replicants: 1
			|	timestamp-dimension: "timestamp"
  		|	rollup:
    	|		granularity: "MINUTE"
    	|		aggregators:
      |			- type: "LONGSUM"
      |  			name: "pageviews"
      |  			field-name: "pv"
      |			- type: "LONGSUM"
      |  			name: "clicks"
      |  			field-name: "ct"
    	|		dimension-exclusions:
      | 		- "pv"
      | 		- "ct"
      | 		- "timestamp"
			""".stripMargin.replaceAll("\t", "	")
			
      println(config_yaml)
      System.exit(1)
    }
    
    val reader = new FileReader(args(0))
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: InputConfig = mapper.readValue(reader, classOf[InputConfig])
    
    val sparkContext = SparkStreamingTasks.startSparkContext(config)
    sparkContext.start()
    sparkContext.awaitTermination()
  }
  
}