package com.atguigu.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

case class Sensor(
                   id: String, timestamp: Long, temperature: Double
                 )

object apitest {
  def main(args: Array[String]): Unit = {
    //1.环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.getConfig.setAutoWatermarkInterval(5000)
    //2.数据源
    //从集合读取
    val inputDS1: DataStream[Sensor] = env.fromCollection(List(
      Sensor("sensor_1", 1547718199, 35.80018327300259),
      Sensor("sensor_6", 1547718201, 15.402984393403084),
      Sensor("sensor_1", 1547718202, 16.80018327300259),
      Sensor("sensor_7", 1547718202, 6.720945201171228)
    ))


    //从文件读取
    val inpath = "F:\\WorkSpace\\inteliJ\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputDS2: DataStream[String] = env.readTextFile(inpath)
    val value: DataStream[String] = inputDS2.union(inputDS2)

    //kafka作为数据源
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop101:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputDS3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    //3.转换
    val transDS: DataStream[Sensor] = inputDS2.map(fun = data => {
      val dataArray = data.split(",")
      Sensor(dataArray(0), dataArray(1).trim().toLong, dataArray(2).trim().toDouble)
    })
      //.assignAscendingTimestamps(_.timestamp*1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.milliseconds(1000)) {
      override def extractTimestamp(t: Sensor): Long = {
        t.timestamp * 1000
      }
    })
    //.keyBy("id").reduce((x, y) => Sensor(x.id, x.timestamp.min(y.timestamp) + 5, x.temperature + y.temperature))
    //输出
    //transDS.print()
    //根据温度高低拆分流
    val splitStream: SplitStream[Sensor] = transDS.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })


    val high: DataStream[Sensor] = splitStream.select("high")
    val low: DataStream[Sensor] = splitStream.select("low")
    val all: DataStream[Sensor] = splitStream.select("high", "low")

    val warning: DataStream[(String, Double)] = high.map(data => (data.id, data.temperature))
    val connected: ConnectedStreams[(String, Double), Sensor] = warning.connect(low)

    val coMapStream: DataStream[Product] = connected.map(warnData => (warnData._1, warnData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    //Sink
    //high.print("high").setParallelism(1)
    //low.print("low").setParallelism(1)
    //all.print("all").setParallelism(1)
    //warning.print("warning").setParallelism(1)
    //coMapStream.print("coMap").setParallelism(1)

    //kafka sink


    //统计每3个数据里的最小温度
    val minDS: DataStream[(String, Double)] = transDS.map(data => {
      (data.id, data.temperature)
    }).keyBy(_._1).countWindow(3, 1)
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))
    minDS.print("min").setParallelism(1)

    env.execute("API test")

  }
}
