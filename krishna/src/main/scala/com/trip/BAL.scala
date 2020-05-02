package com.trip
import org.apache.spark.sql._;
import org.apache.spark.sql.expressions._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.{ Encoder, Encoders }

class BAL(spark: SparkSession, tripData: Dataset[trip]) {

  import spark.implicits._


  
  val PaymentTypes: DataFrame = {    
    val PaymentTypes = Array(
                              (1, "Credit card"),
                              (2, "Cash"),
                              (3, "No charge"),
                              (4, "Dispute"),
                              (5, "Unknown"),
                              (6, "Voided trip")
                            ); 
    spark.createDataFrame(PaymentTypes).select(col("_1").alias("PaymentId"), col("_2").alias("Payment mode"));
    
  }
  
  val Vendors: DataFrame = {    
    val Vendors = Array(
                              (1, "Creative Mobile Technologies"),
                              (2, " VeriFone Inc")
                            ); 
    spark.createDataFrame(Vendors).select(col("_1").alias("VendorID"), col("_2").alias("Vendor"));
    
  }
  
  val RateCodes: DataFrame = {    
    val RateCodes = Array(
                  (1,"Standard rate"),
                  (2,"JFK"),
                  (3,"Newark"),
                  (4,"Nassau or Westchester"),
                  (5,"Negotiated fare"),
                  (6,"Group ride")
               ); 
    spark.createDataFrame(RateCodes).select(col("_1").alias("RateCodeID"), col("_2").alias("RateCode"));
    
  }
  
  
  
  //1. Find the peak time of pickup/drops on a daily basis. Consider peak time to be interval of 1 hour.
  def getTrip_pickup_dropOff_peekHours(): Dataset[Row] = {

    val w1 = Window.partitionBy("tpep_pickup").orderBy(asc("tpep_pickup"), desc("TotalPickups"))

    val df1 = tripData.select("tpep_pickup", "tpep_pickup_hour")
      .groupBy("tpep_pickup", "tpep_pickup_hour")
      .agg(expr("count(tpep_pickup_hour) as TotalPickups"))
      .withColumn("rank", rank().over(w1))
      .where("rank=1")
      .select("tpep_pickup", "tpep_pickup_hour");

    val w2 = Window.partitionBy("tpep_dropoff").orderBy(asc("tpep_dropoff"), desc("Totaldropoffs"))

    val df2 = tripData.select("tpep_dropoff", "tpep_dropoff_hour")
      .groupBy("tpep_dropoff", "tpep_dropoff_hour")
      .agg(expr("count(tpep_dropoff_hour) as Totaldropoffs"))
      .withColumn("rank", rank().over(w2))
      .where("rank=1")
      .select("tpep_dropoff", "tpep_dropoff_hour");

    df1.join(df2, df1.col("tpep_pickup") === df2.col("tpep_dropoff"))
    .sort("tpep_pickup")
    .select(
        col("tpep_pickup").alias("Pickup Date"),
        col("tpep_pickup_hour").alias("Pickup Peek Hour"),
        col("tpep_dropoff").alias("DropOff Date"),
        col("tpep_dropoff_hour").alias("DropOff Peek Hour"),
    );
    

  }

  //2. Find the average total amount per payment type.
  def get_average_total_amount_per_payment(): Dataset[Row] = {

   val df1= tripData.groupBy($"payment_type").agg("total_amount" -> "avg");
   
   df1.join(PaymentTypes,df1.col("payment_type")===PaymentTypes.col("PaymentId"))
   .select("Payment mode", "avg(total_amount)");

  }

  //3. Find anomalies in the average trip distance per Vendor Id.
  def get_anomalies_average_trip_distance_per_Vendor_Id(): Dataset[Row] = {

    tripData.groupBy($"VendorID").agg("trip_distance" -> "avg")
      .withColumnRenamed("avg(trip_distance)", "avg_vendorid_trip_distance")
      .join(tripData.agg("trip_distance" -> "avg"))
      .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")
      .filter("avg_trip_distance<avg_vendorid_trip_distance");

  }

  //4. If the trip distance for a vendor Id increases above the mean value mark the trip as anomalous. Assume period of 3 days for the trips.
  def get_anomalies_trips(): Dataset[Row] = {

    val df1 = tripData.withColumn("tpep_pickup", $"tpep_pickup_datetime".cast("date"))
      .select("VendorID", "tpep_pickup", "trip_distance");

    df1.createOrReplaceTempView("tripdata")

    spark.sql("select " +
      " VendorID, " +
      " tpep_pickup, " +
      " AVG(trip_distance) mean_vendor_trip_distance " +
      " from tripdata " +
      " group by tpep_pickup,VendorID")
      .join(df1.agg("trip_distance" -> "avg"))
      .withColumnRenamed("avg(trip_distance)", "mean_trip_distance")
      .createOrReplaceTempView("tripdata");

   val df2= spark.sql("select " +
      " VendorID, " +
      " tpep_pickup, " +
      " mean_vendor_trip_distance, " +
      " mean_trip_distance, " +
      " case when mean_vendor_trip_distance>mean_trip_distance then 'anomalous' else '' end tripInfo " +
      " from tripdata order by tpep_pickup ASC")
      
      df2.join(Vendors,df2.col("VendorID")===Vendors("VendorID"))
      .select(
          col("Vendor"), 
          col("tpep_pickup").alias("Pickup Date"),
          col("mean_vendor_trip_distance"),
          col("mean_trip_distance"),
          col("tripInfo")
          );

  }

  
  //5. Find the average trip distance per RateCodeId.
  def get_average_trip_distance_per_RateCodeId(): Dataset[Row] = {

   val df1= tripData.groupBy($"RatecodeID").agg("trip_distance" -> "avg")
      .withColumnRenamed("avg(trip_distance)", "avg_ratecode_trip_distance")
      
      df1.join(RateCodes,df1.col("RatecodeID")===RateCodes.col("RatecodeID"))
      .select(
          col("Ratecode"), col("avg_ratecode_trip_distance"));

  }

  //6. Rank the Vendor Id/per day based on the fare amount. 
  def get_Rank_Vendor_Id_perDay_fare_amount(): Dataset[Row] = {

    val df = tripData
      .withColumn("tpep_pickup", $"tpep_pickup_datetime".cast("date"))
      .select("VendorID", "tpep_pickup", "fare_amount")

    df.createOrReplaceTempView("tripdata");

   val df1= spark.sql("With tb as ( " +
      " SELECT " +
      " VendorID " +
      " ,tpep_pickup  " +
      " ,SUM(fare_amount) total_fare_amount " +
      " FROM tripdata " +
      " group by VendorID,tpep_pickup) " +
      " select VendorID,tpep_pickup,total_fare_amount, " +
      " RANK() over(partition by tpep_pickup order by tpep_pickup ASC,total_fare_amount DESC) rank " +
      " from tb order by tpep_pickup ASC,total_fare_amount DESC");
   
   df1.join(Vendors,df1.col("VendorID")===Vendors.col("VendorID"))
   .select(col("Vendor"), col("tpep_pickup").alias("pickup date"),col("total_fare_amount"),col("rank"));

  }

  //7. Categorise the trips based on its total amount to high, medium and low.
  def get_fare_amount_ranges(): Dataset[Row] = {

    val df = tripData.filter("VendorID is not null")
      .withColumn("tpep_pickup", $"tpep_pickup_datetime".cast("date"))
      .select("VendorID", "tpep_pickup", "fare_amount")

    df.createOrReplaceTempView("tripdata");

   val df1= spark.sql("select VendorID,tpep_pickup,fare_amount " +
      " ,case when fare_amount<10 then 'low' when fare_amount<100 then 'medium' else 'high' end range  " +
      " from tripdata order by fare_amount desc");
   
      df1.join(Vendors,df1.col("VendorID")===Vendors.col("VendorID"))
   .select(col("Vendor"), col("tpep_pickup").alias("pickup date"),col("fare_amount"),col("range"));
   
   
  }
  
   

}