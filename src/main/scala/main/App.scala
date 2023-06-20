package main

// Start mit: env JAVA_OPTS="-Xmx4g" sbt run

/*
Programm endet, wenn mit sbt gestartet, mit einer Exception.
Das liegt daran, dass sbt und das Programm in der selben JVM laufen.
Wenn das Programm mit einem exit unterbrochen wird, dann kommt es zu der Fehlermeldung:
java.lang.InterruptedException
	at java.lang.Object.wait(Native Method)
	at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:144)
	at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:165)
	at org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner.run(FileSystem.java:2989)
	at java.lang.Thread.run(Thread.java:748)
Fehlermeldung tritt nicht auf, wenn mit Spark-Submit gestartet wird.
 */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.time.OffsetDateTime
import loganalyse._
import org.jfree.ui.ApplicationFrame

object App extends App {

  println("Analyse des NASA-Logfiles vom August 1995")
  val spark: SparkSession = SparkSession.builder().
    master("local").
    appName("LogAnalyse").
    getOrCreate()
  val sc = spark.sparkContext

  val logs = Utilities.getData("NASA_access_log_Aug95.txt", "resources", sc)
  val (parsed_logs, access_logs, failed_logs) = LogAnalyseFuns.getParsedData(logs)

  println("Stastitik")
  println("Anzahl der geparsten Datensätze:" + access_logs.count)
  println("Anzahl der fehlerhaften Datensätze:" + failed_logs.count)
  println("Die ersten 10 fehlerhaften Datensätze:")
  failed_logs.take(10).foreach(println)

  println("Analyse der relativen Häufigkeiten der Response Codes")
  val responseCodes = LogAnalyseFuns.getResponseCodesAndFrequencies(access_logs)
  val appframe1 = Graphs.createPieChart(responseCodes)
  println("Please press enter....")
  System.in.read()
  appframe1.setVisible(false)
  appframe1.dispose

  println("Analyse der Requests pro Tag")
  val requestsPerDay = LogAnalyseFuns.getNumberOfRequestsPerDay(access_logs)
  val appframe2 = Graphs.createLineChart(requestsPerDay, "Requests Per Day", "Tag", "Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe2.setVisible(false)
  appframe2.dispose()

  println("Analyse der Errors pro Tag")
  val errorCodesPerDay = LogAnalyseFuns.responseErrorCodesPerDay(access_logs)
  val appframe3 = Graphs.createLineChart(errorCodesPerDay, "Error Codes Per Day", "Tag", "Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe3.setVisible(false)
  appframe3.dispose

  println("Durchschnittliche Anzahl der Requests pro Host und Tag")
  val avgNrOfRequestsPerDayAndHost = LogAnalyseFuns.averageNrOfDailyRequestsPerHost(access_logs)
  val appframe4 = Graphs.createLineChart(avgNrOfRequestsPerDayAndHost, "Average Number Of Requests Per Day and Host", "Tag", "Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe4.setVisible(false)
  appframe4.dispose

  println("Durchschnittliche Anzahl der Requests pro Wochentag")
  val avgNrOfRequestsPerWeekDay = LogAnalyseFuns.getAvgRequestsPerWeekDay(access_logs)
  val appframe5 = Graphs.createBarChart(avgNrOfRequestsPerWeekDay, "Average Number Of Requests Per Weekday", "Tag", "Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe5.setVisible(false)
  appframe5.dispose

  sc.stop
}