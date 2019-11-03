package com.br.bigdatamasters.stocks

import org.slf4j.{Logger,LoggerFactory}
import scala.collection.mutable.ArrayBuffer
import java.net.{URL, HttpURLConnection}

class StockAPI {

  val logger = LoggerFactory.getLogger(classOf[StockAPI])
  val apiKey : String = "k9B22eS1vQvID3CK2rL3WafGw5FJJZibxwORjSUo9ilM5f0kblDjbvkqY1ut"
  val stocks : ArrayBuffer[String] = new ArrayBuffer[String]()
  var url : String = _

  def this(stocks : String*) {
    this()
    this.stocks.append(stocks:_*)
    this.url = s"https://api.worldtradingdata.com/api/v1/stock?symbol=${this.stocks.mkString(",")}&api_token=${this.apiKey}"
  }

  def get(connectTimeout: Int = 5000,
          readTimeout: Int = 5000,
          requestMethod: String = "GET") =
  {

    if(this.stocks.length == 0) new Throwable("Stocks cannot be null")

    logger.info(s"Sending Request for Stocks : ${this.stocks.mkString(",")} ")
    val connection = (new URL(this.url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  }

}
