package com.br.bigdatamasters.stocks

import java.io.FileInputStream

import org.slf4j.{Logger, LoggerFactory}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import java.util.Properties
import com.br.bigdatamasters.stocks.StockAPI

class StockProducer {

  val logger = LoggerFactory.getLogger(classOf[StockProducer])
  val config = new Properties()
  config.load(new FileInputStream("/home/vitor/IdeaProjects/StockProducer/src/main/resources/properties.config"))
  val producer = new KafkaProducer[String,String](config)
  var stockApi : StockAPI = _

  def this(stockAPI : StockAPI) = {
    this()
    this.stockApi = stockAPI
  }

  def sendData(): Unit = {

    while(true) {
      var message = this.stockApi.get()
      var record = new ProducerRecord[String,String]("ev-stock",message)

      this.producer.send(record,new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if(e != null) {
            e.printStackTrace()
          } else {
            logger.info(s"Sent Message ${recordMetadata.offset()} to StockHub")
          }
        }
      })

      Thread.sleep(60000)
    }

  }

}
