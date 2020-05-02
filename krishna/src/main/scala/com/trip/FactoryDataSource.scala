package com.trip
import org.apache.spark.sql.SparkSession;

class FactoryDataSource(SparkSession: SparkSession, FactoryDataSource: Int, inputSource: String) {



}
object FactoryDataSource {

  val CSV = 0
  val HIVE = 1
  val ODBC = 2
  val JDBC = 3


  def apply(SparkSession: SparkSession, FactoryDataSource: Int, inputSource: String): DataSource = {

    FactoryDataSource match {
      case 0 => CSVDataSource(SparkSession, inputSource);
      case 1 => HIVEDataSource(SparkSession, inputSource);
      case _ => null;

    }

  }
}