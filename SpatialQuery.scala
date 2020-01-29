package cse512

import org.apache.spark.sql.SparkSession
import scala.math._

object SpatialQuery extends App{

  //def square(b:Double): Double={
  //  return b*b;
  //}
  def ST_Contains(queryRectangle:String, pointString:String): Boolean ={

    var points = pointString.split(",")
    var x_point = points(0).toDouble
    var y_point = points(1).toDouble
    var rectangleCoordinates = queryRectangle.split(",")
    var x_1 = rectangleCoordinates(0).toDouble
    var y_1 = rectangleCoordinates(1).toDouble
    var x_2 = rectangleCoordinates(2).toDouble
    var y_2 = rectangleCoordinates(3).toDouble
    var trY = y_2
    var bly = y_1
    var trx = x_2
    var blx = min(x_1, x_2)
    if(blx == x_2){
      bly = y_2
      trx = x_1
      trY = y_1
    }
    var x_fit = (x_point >= blx && x_point <= trx)
    var y_fit = (y_point >= bly && y_point <= trY)

    return (x_fit && y_fit)
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean ={
    var points_1 = pointString1.split(",")
    var x_point_1 = points_1(0).toDouble
    var y_point_1 = points_1(1).toDouble
    var points_2 = pointString2.split(",")
    var x_point_2 = points_2(0).toDouble
    var y_point_2 = points_2(1).toDouble
    var Actual_Distance  = sqrt(pow(x_point_2 - x_point_1, 2) + pow(y_point_2 - y_point_1, 2))

    return (Actual_Distance <= distance)

  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //println("before calling st contains runRangeQuery ")
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ((ST_Contains(queryRectangle, pointString))))
    //println("after calling st contains runRangeQuery ")


    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    print("Inside runRangeQuery")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //println("before calling st contains runRangeJoinQuery ")
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))
    //println("after calling st contains runRangeJoinQuery")

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    print("Inside runRangeJoinQuery")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //println("before calling st ST_Within runDistanceQuery ")
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    //println("after calling st ST_Within runDistanceQuery ")

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    print("Inside runDistanceQuery")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //println("before calling st ST_Within runDistanceJoinQuery ")
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    //println("after calling st ST_Within runDistanceJoinQuery ")

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    print("Inside runDistanceJoinQuery")
    resultDf.show()

    return resultDf.count()
  }
}