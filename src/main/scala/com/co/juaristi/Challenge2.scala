package com.co.juaristi

import java.time.LocalDate

import org.apache.spark.sql.DataFrame


trait Challenge2 {
  import org.apache.spark.sql.functions._

  /**
   * 1. ¿Cuales son las diferentes acciones de los últimos 6 meses?, ¿cuál es su moneda(currency) y cúal es su valor promedio?
   * Para el resultado tener en cuenta: Mnemonic, Currency, avg(EndPrice)
   */
  def last6monthTrades(df: DataFrame): DataFrame = {

    import df.sparkSession.implicits._

    val date = LocalDate.now().minusMonths(6)

    df
      .where(to_date($"Date") > lit(date.toString))
      .groupBy("Mnemonic", "Currency")
      .agg(avg("EndPrice"))
      .sort($"Avg".desc)
  }

  /**
   * 2. Buscar en los últimos tres meses cuáles acciones con más ventas y el precio promedio por cada mes
   *
   * Filtrar del contenido de los archivos las lineas donde NumberOfTrades > 50
   *
   * | Trade | currency | Dec-19 | Jan-20 | Feb-20 |
   * |-------|----------|--------|--------|--------|
   *
   */
  def lastThreeMonthTradesAveragePrice(df: DataFrame): DataFrame = {

    import df.sparkSession.implicits._

    df
      .where($"NumberOfTrades" > 50)
      .withColumn("Month", date_format($"Date", "MM-yyyy"))
      .groupBy("Mnemonic", "Currency" )
      .agg(
        avg("EndPrice"),
        avg("EndPrice").as("otro")
      )
      .show()
    df
  }

  /**
   * 3. De un listado de acciones,  listar el valor de apertura, de cierre, el valor mínimo y el valor máximo de la última semana
   * Para el resultado tener en cuenta: Mnemonic, StartPrice, EndPrice y Time
   * | Date | Trade | opening price | closing price| min price | max price |
   *
   */
  def openingClosingPriceTrades(df: DataFrame, trades: Seq[String]): DataFrame = {

    import df.sparkSession.implicits._

    val date = LocalDate.now().minusDays(7)
    df
      .where(to_date($"Date") > lit(date.toString))
      .where($"Mnemonic".isin(trades:_*))
      .groupBy("Mnemonic")
      .agg(
        first("StartPrice").as("opening price"),
        last("EndPrice").as("closing price"),
        min("MinPrice").as("min price"),
        max("MaxPrice").as("max price")
      )
  }

  /**
   *  4. Obtenga la tendencia de una acciones en los últimos dias con respecto al día anterior,
   * de esta manera determinar si el valor subió, bajó o se mantuvo.
   * Para el resultado tener en cuenta: Mnemonic, avg(EndPrice)
   */
  def tradeTrending(df: DataFrame): DataFrame = ???
}