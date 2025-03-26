import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try

object InferTypes {
  def inferPropertyTypesFromMerged(
    originalDF: DataFrame, 
    mergedDF: DataFrame, 
    name: String, 
    propertyCols: Seq[String], 
    idCol: String
  ): DataFrame = {
    import originalDF.sparkSession.implicits._

    val allPropertiesDFs = propertyCols.map { colName =>
      val colType = mergedDF.schema(colName).dataType
      colType match {
        case ArrayType(ArrayType(StringType, _), _) =>
          mergedDF.select(explode(flatten(col(colName))).as("property")).distinct()
        case ArrayType(StringType, _) =>
          mergedDF.select(explode(col(colName)).as("property")).distinct()
        case _ =>
          mergedDF.select(lit(null).as("property"))
      }
    }

    val allPropertiesRaw = if (allPropertiesDFs.nonEmpty) {
      allPropertiesDFs.reduce((df1, df2) => df1.union(df2))
        .distinct()
        .collect()
        .filter(row => !row.isNullAt(0))
        .map(_.getString(0))
        .toSeq
    } else {
      Seq.empty[String]
    }

    val allProperties = allPropertiesRaw.map { prop =>
      if (prop.startsWith("prop_")) prop.stripPrefix("prop_") else prop
    }.filter(prop => originalDF.columns.contains(prop) && prop != "original_label" ).distinct


    println(s"Extracted properties: ${allPropertiesRaw.mkString(", ")}")
    println(s"Adjusted properties for $name: ${allProperties.mkString(", ")}")


    val sampleDF = originalDF.limit(1000).cache()
    println(s"SampleDF count: ${sampleDF.count()}")
    sampleDF.show(5)

    def isInteger(str: String): Boolean = Try(str.trim.toInt).isSuccess
    def isDouble(str: String): Boolean = Try(str.trim.toDouble).isSuccess
    def isDate(str: String): Boolean = {
      val formats = Seq("yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "yyyy/MM/dd")
      formats.exists(fmt => Try(new SimpleDateFormat(fmt).parse(str.trim)).isSuccess)
    }
    def isBoolean(str: String): Boolean = {
      val boolValues = Set("true", "false", "yes", "no", "1", "0")
      boolValues.contains(str.trim.toLowerCase)
    }


    val inferredTypes = allProperties.map { prop =>
      val values = sampleDF.select(prop)
        .na.drop()
        .collect()
        .map(_.getString(0))
        .toSeq
      println(s"Values for $prop: ${values.take(5).mkString(", ")} (total: ${values.length})")

      if (values.isEmpty) {
        (prop, "Unknown (empty)")
      } else {
        val allIntegers = values.forall(isInteger)
        val allDoubles = values.forall(isDouble)
        val allDates = values.forall(isDate)
        val allBooleans = values.forall(isBoolean)

        val inferredType = if (allIntegers) "Integer"
        else if (allDoubles) "Double"
        else if (allDates) "Date"
        else if (allBooleans) "Boolean"
        else "String"

        (prop, inferredType)
      }
    }.toMap

    println(s"\nInferred Property Types for $name:")
    inferredTypes.foreach { case (prop, typ) => println(s"Property: $prop, Inferred Type: $typ") }


    val typeMapUdf = udf((properties: Seq[String]) => 
      if (properties == null || properties.isEmpty) 
        Seq.empty[String]
      else 
        properties.map { prop =>
          val baseProp = if (prop.startsWith("prop_")) prop.stripPrefix("prop_") else prop
          s"$prop:${inferredTypes.getOrElse(baseProp, "Unknown")}"
        }
    )

    val updatedDF = propertyCols.foldLeft(mergedDF) { (df, colName) =>
      val colType = df.schema(colName).dataType
      colType match {
        case ArrayType(ArrayType(StringType, _), _) =>
          df.withColumn(s"${colName}_with_types", typeMapUdf(flatten(col(colName))))
        case ArrayType(StringType, _) =>
          df.withColumn(s"${colName}_with_types", typeMapUdf(col(colName)))
        case _ =>
          df.withColumn(s"${colName}_with_types", lit(null).cast(ArrayType(StringType)))
      }
    }

    updatedDF
  }
}