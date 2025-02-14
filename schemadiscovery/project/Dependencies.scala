import sbt._

object Dependencies {
  val scalaVer = "2.12.10" 
  val munitVer = "0.7.29"
  val sparkVer = "3.4.1"

  val munit = "org.scalameta" %% "munit" % munitVer
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVer
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVer
  val mllib = "org.apache.spark" %% "spark-mllib" % sparkVer
  
}