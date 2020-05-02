package com.trip
import org.apache.spark.sql._

object DataStore {
  
    def SaveToCSV(Rpt: Dataset[Row], OutputDirectory: String, ReportName: String) = {

    var strOutputDirectory = OutputDirectory
    if (strOutputDirectory.length() > 0 && strOutputDirectory.substring(strOutputDirectory.length() - 1) != "/")
      strOutputDirectory = strOutputDirectory + "/";

    if(strOutputDirectory=="")
    {
      strOutputDirectory="reports/";
    }
    
    
    Rpt.coalesce(1).write.option("header", "true").csv(strOutputDirectory + ReportName);

  }
}