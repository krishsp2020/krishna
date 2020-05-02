package com.trip
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ col, hour }
class HIVEDataSource(SparkSession: SparkSession, val hiveTableName: String) extends DataSource {

  val spark = SparkSession;

  import spark.implicits._

  val tripData: Dataset[trip] = {

    //read csv data
    val df = spark.sql("select * from " + hiveTableName)
      .filter("VendorID is not null")
      .withColumn("tpep_pickup", $"tpep_pickup_datetime".cast("date"))
      .withColumn("tpep_pickup_hour", hour($"tpep_pickup_datetime"))
      .withColumn("tpep_dropoff", $"tpep_dropoff_datetime".cast("date"))
      .withColumn("tpep_dropoff_hour", hour($"tpep_dropoff_datetime"))
      .withColumn("tpep_pickup_datetime", $"tpep_pickup_datetime".cast("timestamp"))
      .withColumn("tpep_dropoff_datetime", $"tpep_dropoff_datetime".cast("timestamp"));

    df.as[trip]
  }

  def getTripDatase(): Dataset[trip] = {
    return tripData;
  }

 
  def getTrip_pickup_dropOff_peekHours(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.getTrip_pickup_dropOff_peekHours();

  }
  def get_average_total_amount_per_payment(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.get_average_total_amount_per_payment();

  }

  def get_anomalies_average_trip_distance_per_Vendor_Id(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.get_anomalies_average_trip_distance_per_Vendor_Id();
  }

  def get_anomalies_trips(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.get_anomalies_trips();

  }

  def get_average_trip_distance_per_RateCodeId(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.get_average_trip_distance_per_RateCodeId();
  }

  def get_Rank_Vendor_Id_perDay_fare_amount(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.get_Rank_Vendor_Id_perDay_fare_amount();
  }

  def get_fare_amount_ranges(): Dataset[Row] = {

    val bal = new BAL(spark, tripData);

    bal.get_fare_amount_ranges();

  }

}
object HIVEDataSource {

  def apply(SparkSession: SparkSession, filePath: String): DataSource = {

    new HIVEDataSource(SparkSession, filePath);

  }
}