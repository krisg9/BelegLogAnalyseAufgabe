package loganalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object LogAnalyseFuns {

  def getParsedData(data: RDD[String]): (RDD[(Row, Int)], RDD[Row], RDD[Row]) = {

    val parsed_logs = data.map(Utilities.parse_line(_))
    val access_logs = parsed_logs.filter(_._2 == 1).map(_._1).cache()
    val failed_logs = parsed_logs.filter(_._2 == 0).map(_._1)
    (parsed_logs, access_logs, failed_logs)
  }

  /*
   * Calculate for the content size the following values:
   *
   * minimum: Minimum value
   * maximum: Maximum value
   * average: Average
   *
   * Return the following triple:
   * (min,max,avg)
   */
  def calculateLogStatistic(data: RDD[Row]): (Long, Long, Long) = {
    val contentSize: RDD[Long] = data.map(row => row.getInt(8).toLong)
    (contentSize.min(), contentSize.max(), contentSize.mean().toLong)
  }

  /*
   * Calculate for each single response code the number of occurrences
   * Return a list of tuples which contain the response code as the first
   * element and the number of occurrences as the second.
   *    *
   */
  def getResponseCodesAndFrequencies(data: RDD[Row]): List[(Int, Int)] = {
    val codes: RDD[Int] = data.map(row => row.getInt(7))
    codes.map(x => (x, 1)).reduceByKey((x, y) => x + y).collect().toList
  }

  /*
   * Calculate 20 arbitrary hosts from which the web server was accessed more than 10 times
   * Print out the result on the console (no tests take place)
   */
  def get20HostsAccessedMoreThan10Times(data: RDD[Row]): List[String] = {
    val hosts = data.map(row => row.getString(0))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .filter(tuple => tuple._2 > 10)
      .map(_._1)
      .takeSample(withReplacement = false, 20).toList
    println(hosts)
    hosts
  }

  /*
   * Calculate the top ten endpoints.
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of accesses as the second
   * The list should be ordered by the number of accesses.
   */
  def getTopTenEndpoints(data: RDD[Row]): List[(String, Int)] = {
    data.map(row => row.getString(5))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .takeOrdered(10)(Ordering.by(-_._2))
      .toList
  }

  /*
   * Calculate the top ten endpoint that produces error response codes (response code != 200).
   *
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of errors as the second.
   * The list should be ordered by the number of accesses.
   */
  def getTopTenErrorEndpoints(data: RDD[Row]): List[(String, Int)] = {
    data.map(row => (row.getString(5), row.getInt(7)))
      .map(x => (x, 1))
      .filter(y => y._1._2 != 200)
      .map(x => (x._1._1, x._2))
      .reduceByKey((x, y) => x + y)
      .takeOrdered(10)(Ordering.by(-_._2))
      .toList
  }

  /*
   * Calculate the number of requests per day.
   * Return a list of tuples which contain the day (1..30) as the first element and the number of
   * accesses as the second.
   * The list should be ordered by the day number.
   */
  def getNumberOfRequestsPerDay(data: RDD[Row]): List[(Int, Int)] = ???

  /*
   * Calculate the number of hosts that accesses the web server in June 95.
   * Every hosts should only be counted once.
   */
  def numberOfUniqueHosts(data: RDD[Row]): Long = ???

  /*
  * Calculate the number of hosts per day that accesses the web server.
  * Every host should only be counted once per day.
  * Order the list by the day number.
  */
  def numberOfUniqueDailyHosts(data: RDD[Row]): List[(Int, Int)] = ???

  /*
   * Calculate the average number of requests per host for each single day.
   * Order the list by the day number.
   */
  def averageNrOfDailyRequestsPerHost(data: RDD[Row]): List[(Int, Int)] = ???

  /*
     * Calculate the top 25 hosts that causes error codes (Response Code=404)
     * Return a set of tuples consisting the hostnames  and the number of requests
     */
  def top25ErrorCodeResponseHosts(data: RDD[Row]): Set[(String, Int)] = ???

  /*
   * Calculate the number of error codes (Response Code=404) per day.
   * Return a list of tuples that contain the day as the first element and the number as the second.
   * Order the list by the day number.
   */
  def responseErrorCodesPerDay(data: RDD[Row]): List[(Int, Int)] = ???

  /*
   * Calculate the error response coded for every hour of the day.
   * Return a list of tuples that contain the hour as the first element (0..23) abd the number of error codes as the second.
   * Ergebnis soll eine Liste von Tupeln sein, deren erstes Element die Stunde bestimmt (0..23) und
   * Order the list by the hour-number.
   */
  def errorResponseCodeByHour(data: RDD[Row]): List[(Int, Int)] = ???

  /*
     * Calculate the number of requests per weekday (Monday, Tuesday,...).
     * Return a list of tuples that contain the number of requests as the first element and the weekday
     * (String) as the second.
     * The elements should have the following order: [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday].
     */
  def getAvgRequestsPerWeekDay(data: RDD[Row]): List[(Int, String)] = ???
}