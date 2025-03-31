import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try

object InferSchema {
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
    }.filter(prop => originalDF.columns.contains(prop) && prop != "original_label").distinct

    println(s"Extracted properties: ${allPropertiesRaw.mkString(", ")}")
    println(s"Adjusted properties for $name: ${allProperties.mkString(", ")}")


    def isInteger(str: String): Boolean = Try(str.trim.toInt).isSuccess
    def isDouble(str: String): Boolean = Try(str.trim.toDouble).isSuccess
    def isDate(str: String): Boolean = {
      val formats = Seq("yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "yyyy/MM/dd", "yyyy", "yyyy/MM", "MM/yyyy")
      formats.exists(fmt => Try(new SimpleDateFormat(fmt).parse(str.trim)).isSuccess)
    }
    def isBoolean(str: String): Boolean = {
      val boolValues = Set("true", "false", "yes", "no", "1", "0")
      boolValues.contains(str.trim.toLowerCase)
    }
    def isIP(str: String): Boolean = {
      val ipPattern = """^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""".r
      ipPattern.findFirstIn(str.trim).isDefined && str.split("\\.").forall(part => Try(part.toInt).isSuccess && part.toInt >= 0 && part.toInt <= 255)
    }

    val inferredTypes = allProperties.map { prop =>
      val sampleDF = originalDF
        .filter(col(prop).isNotNull)
        .limit(1000)
        .cache()

      val sampleCount = sampleDF.count()
      println(s"Sampling $prop: $sampleCount rows with non-null values")

      val values = sampleDF.select(prop)
        .collect()
        .map(_.getString(0))
        .toSeq

      println(s"Values for $prop: ${values.take(5).mkString(", ")} (total: ${values.length})")

      val inferredType = if (values.isEmpty) {
        "Unknown (empty)"
      } else {
        val allIntegers = values.forall(isInteger)
        val allDoubles = values.forall(isDouble)
        val allDates = values.forall(isDate)
        val allBooleans = values.forall(isBoolean)

        if (prop == "locationIP" || values.forall(isIP)) "String"
        else if (allIntegers) "Integer"
        else if (allDoubles) "Double"
        else if (allDates) "Date"
        else if (allBooleans) "Boolean"
        else "String"
      }

      sampleDF.unpersist()
      (prop, inferredType)
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

    println("Updated Merged Patterns LSH with Types:")
    updatedDF.printSchema()
    updatedDF.show(5)

    updatedDF
  }


  def inferCardinalities(edgesDF: DataFrame, mergedEdges: DataFrame): DataFrame = {
    import edgesDF.sparkSession.implicits._

    // scr->dst cardinality
    val srcToDst = edgesDF.groupBy("relationshipType", "srcId")
      .agg(countDistinct("dstId").as("dstCount"))
      .groupBy("relationshipType")
      .agg(
        max("dstCount").as("maxDstPerSrc"),
        min("dstCount").as("minDstPerSrc"),
        avg("dstCount").as("avgDstPerSrc")
      )
    // dst->src cardinality
    val dstToSrc = edgesDF.groupBy("relationshipType", "dstId")
      .agg(countDistinct("srcId").as("srcCount"))
      .groupBy("relationshipType")
      .agg(
        max("srcCount").as("maxSrcPerDst"),
        min("srcCount").as("minSrcPerDst"),
        avg("srcCount").as("avgSrcPerDst")
      )

    val cardinalityDF = srcToDst.join(dstToSrc, "relationshipType")
    cardinalityDF.show()

    val cardinalities = cardinalityDF.collect().map { row =>
      val relType = row.getAs[String]("relationshipType")
      val maxDstPerSrc = row.getAs[Long]("maxDstPerSrc")
      val maxSrcPerDst = row.getAs[Long]("maxSrcPerDst")
      val minDstPerSrc = row.getAs[Long]("minDstPerSrc")
      val minSrcPerDst = row.getAs[Long]("minSrcPerDst")

      val cardinality = (maxDstPerSrc, maxSrcPerDst) match {
        case (1, 1) => "1:1"
        case (dst, 1) if dst > 1 => "1:N"
        case (1, src) if src > 1 => "N:1"
        case (dst, src) if dst > 1 && src > 1 => "N:N"
        case _ => "Unknown"
      }

      (relType, cardinality)
    }.toMap

    println("\nInferred Cardinalities for Edges:")
    cardinalities.foreach { case (relType, card) => println(s"Relationship: $relType, Cardinality: $card") }

    val cardinalityUdf = udf((relTypes: Seq[String]) =>
      relTypes.map(relType => cardinalities.getOrElse(relType, "Unknown")).mkString(",")
    )

    val updatedMergedEdges = mergedEdges.withColumn(
      "cardinality",
      cardinalityUdf(col("relationshipTypes"))
    )

    updatedMergedEdges
  }
}