package com.br.bigdatamasters.stocks


import com.br.bigdatamasters.stocks.{StockAPI,StockProducer}

object Invoker {

  def main(args : Array[String]) : Unit = {

    val producer = new StockProducer(new StockAPI("AAPL,MSFT,HSBA.L,AMZN"))

    producer.sendData()

  }

}
