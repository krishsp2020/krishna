package com.trip
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row
trait DataSource {

  def getTripDatase(): Dataset[trip];
 
  def getTrip_pickup_dropOff_peekHours(): Dataset[Row];
  
  def get_average_total_amount_per_payment(): Dataset[Row];
  
  def get_anomalies_average_trip_distance_per_Vendor_Id(): Dataset[Row];
  
  def get_anomalies_trips(): Dataset[Row];
  
  def get_average_trip_distance_per_RateCodeId(): Dataset[Row];
  
  def get_Rank_Vendor_Id_perDay_fare_amount(): Dataset[Row];
  
  def get_fare_amount_ranges(): Dataset[Row];


}