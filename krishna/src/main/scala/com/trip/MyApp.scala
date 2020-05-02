package com.trip
import java.util.Date;
import org.apache.spark.sql._;
import org.apache.log4j._

object MyApp {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Please pass input CSV file path and output directo to save reports");
      System.exit(1)
    }

    val filePath: String = args(0); // "file:///C:/temp/hpe/source/yellow_tripdata_2018-01.csv" //input CSV file path (yellow_tripdata_2018-01.csv)

    val outputdirectory: String = args(1); //  "file:///C:/temp/hpe/destination/"; //specify directory where reports will be saved

    println("input file: " + filePath);

    println("output directory: " + outputdirectory);

    val d1 = new Date;

    Logger.getLogger("MyApp").setLevel(Level.INFO)

    val spark: SparkSession = SparkSession.builder()
      .appName("Taxi trips data aggregations").getOrCreate();

    val obj = FactoryDataSource(spark, FactoryDataSource.CSV, filePath);

    val outPutReportDirectory = outputdirectory;

    //1 [trip peek hours]
    val trip_peek_hours = obj.getTrip_pickup_dropOff_peekHours;
    DataStore.SaveToCSV(trip_peek_hours, outPutReportDirectory, "trip_peek_hours");

    //2 [average total amount per payment mode]
    val avg_total_amount_per_payment = obj.get_average_total_amount_per_payment
    DataStore.SaveToCSV(avg_total_amount_per_payment, outPutReportDirectory, "avg_total_amount_per_payment");

    //3 [anomalies_average_trip_distance_per_Vendor]
    val anomalies_average_trip_distance_per_Vendor = obj.get_anomalies_average_trip_distance_per_Vendor_Id()
    DataStore.SaveToCSV(anomalies_average_trip_distance_per_Vendor, outPutReportDirectory, "anomalies_average_trip_distance_per_Vendor");

    //4 [anomalies_trips]
    val anomalies_trips = obj.get_anomalies_trips()
    DataStore.SaveToCSV(anomalies_trips, outPutReportDirectory, "anomalies_trips");

    //5 [avg_trip_dist_rateCode]
    val avg_trip_dist_rateCode = obj.get_average_trip_distance_per_RateCodeId()
    DataStore.SaveToCSV(avg_trip_dist_rateCode, outPutReportDirectory, "avg_trip_dist_rateCode");

    //6 [rank_vendor_perday_fare_amount]
    val rank_vendor_perday_fare_amount = obj.get_Rank_Vendor_Id_perDay_fare_amount()
    DataStore.SaveToCSV(rank_vendor_perday_fare_amount, outPutReportDirectory, "rank_vendor_perday_fare_amount");

    //7 [fare_amount_ranges]
    val fare_amount_ranges = obj.get_fare_amount_ranges()
    DataStore.SaveToCSV(fare_amount_ranges, outPutReportDirectory, "fare_amount_ranges");
    
    spark.stop();
    
    val d2 = new Date;

    val timeElapse = (d2.getTime - d1.getTime)

    println("Reports has been created in CSV format to output directory : " + outPutReportDirectory);
    println("Processed in " + (timeElapse.toFloat / 1000) + " seconds")

  }

}