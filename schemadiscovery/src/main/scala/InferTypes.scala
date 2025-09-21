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
    idCol: String,
    evaluateErrorRate: Boolean = true
  ): DataFrame = {
    import originalDF.sparkSession.implicits._

    val allPropertiesDFs = propertyCols.map { colName =>
      val colType = mergedDF.schema(colName).dataType
      colType match {
        case ArrayType(ArrayType(StringType, _), _) =>
          mergedDF.select(explode(flatten(col(colName))).as("property")).distinct()
        case ArrayType(StringType, _ ) =>
          mergedDF.select(explode(col(colName)).as("property")).distinct()
        case _ =>
          mergedDF.select(lit(null).as("property"))
      }
    }

    val allPropertiesRaw =
      if (allPropertiesDFs.nonEmpty)
        allPropertiesDFs.reduce(_ union _)
          .distinct()
          .collect()
          .filter(r => !r.isNullAt(0))
          .map(_.getString(0))
          .toSeq
      else Seq.empty[String]

    val allProperties = allPropertiesRaw
      .map(p => if (p.startsWith("prop_")) p.stripPrefix("prop_") else p)
      .filter(p => originalDF.columns.contains(p) && p != "original_label")
      .distinct

    println(s"Extracted properties: ${allPropertiesRaw.mkString(", ")}")
    println(s"Adjusted properties for $name: ${allProperties.mkString(", ")}")

    def isInteger(str: String): Boolean = Try(str.trim.toInt).isSuccess
    def isDouble (str: String): Boolean = Try(str.trim.toDouble).isSuccess
    def isDate   (str: String): Boolean = {
      val formats = Seq("yyyy-MM-dd","dd/MM/yyyy","MM-dd-yyyy","yyyy/MM/dd","yyyy","yyyy/MM","MM/yyyy")
      formats.exists(f => Try(new SimpleDateFormat(f).parse(str.trim)).isSuccess)
    }
    def isBoolean(str: String): Boolean = {
      val boolValues = Set("true","false","yes","no","1","0")
      boolValues.contains(str.trim.toLowerCase)
    }
    def isIP(str: String): Boolean = {
      val ip = """^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""".r
      ip.findFirstIn(str.trim).isDefined &&
        str.split("\\.").forall(part => Try(part.toInt).isSuccess && part.toInt >= 0 && part.toInt <= 255)
    }

    val inferredTypes = scala.collection.mutable.Map[String, String]()

    if (evaluateErrorRate) {
      val spark = originalDF.sparkSession
      import spark.implicits._

      val isIntegerUdf = udf(isInteger _)
      val isDoubleUdf  = udf(isDouble  _)
      val isDateUdf    = udf(isDate    _)
      val isBooleanUdf = udf(isBoolean _)
      val isIPUdf      = udf(isIP      _)

      case class TypeStats(
        inferredType: String,
        total: Long,
        intCnt: Long,
        dblCnt: Long,
        dateCnt: Long,
        boolCnt: Long,
        ipCnt: Long,
        support: Double
      )

      def computeTypeStats(df: DataFrame, prop: String): TypeStats = {
        val nonNull = df.filter(col(prop).isNotNull)
        val total   = nonNull.count()
        if (total == 0L) return TypeStats("Unknown (empty)", 0L,0L,0L,0L,0L,0L,0.0)

        val counts = nonNull.select(
          sum(when(isIntegerUdf(col(prop)), 1).otherwise(0)).as("intCnt"),
          sum(when(isDoubleUdf (col(prop)), 1).otherwise(0)).as("dblCnt"),
          sum(when(isDateUdf   (col(prop)), 1).otherwise(0)).as("dateCnt"),
          sum(when(isBooleanUdf(col(prop)), 1).otherwise(0)).as("boolCnt"),
          sum(when(isIPUdf     (col(prop)), 1).otherwise(0)).as("ipCnt")
        ).as[(Long,Long,Long,Long,Long)].first()

        val (intCnt, dblCnt, dateCnt, boolCnt, ipCnt) = counts
        val typeToCount = Seq(
          "Integer" -> intCnt,
          "Double"  -> dblCnt,
          "Date"    -> dateCnt,
          "Boolean" -> boolCnt
        )

        val inferredType =
          if (prop == "locationIP" || ipCnt == total) "String"
          else {
            val (bestType, bestCnt) = typeToCount.maxBy(_._2)
            if (bestCnt == 0L) "String" else bestType
          }

        val support = inferredType match {
          case "Integer" => intCnt.toDouble  / total
          case "Double"  => dblCnt.toDouble  / total
          case "Date"    => dateCnt.toDouble / total
          case "Boolean" => boolCnt.toDouble / total
          case "String"  =>
            if (prop == "locationIP" || ipCnt == total) 1.0
            else {
              val bestOther = typeToCount.map(_._2).max
              if (bestOther == 0L) 1.0 else 1.0 - (bestOther.toDouble / total)
            }
          case _ => 0.0
        }

        TypeStats(inferredType, total, intCnt, dblCnt, dateCnt, boolCnt, ipCnt, support)
      }

      val rows =
        allProperties.map { prop =>
          val fullDF   = originalDF.filter(col(prop).isNotNull).cache()
          val sampleDF = fullDF.limit(1000).cache()

          val sampleCount = sampleDF.count()
          println(s"Sampling $prop: $sampleCount rows with non-null values")

          val s = computeTypeStats(sampleDF, prop)
          val f = computeTypeStats(fullDF,   prop)

          inferredTypes(prop) = s.inferredType

          sampleDF.unpersist()
          fullDF.unpersist()

          (prop, s.inferredType, f.inferredType, s.support, f.support, s.total, f.total)
        }

      val metricsDF = rows.toDF(
        "property", "sample_type", "full_type",
        "sample_support", "full_support",
        "sample_n", "full_n"
      ).withColumn("match", $"sample_type" === $"full_type")
      .withColumn("delta", abs($"sample_support" - $"full_support"))

      val totalProps = metricsDF.count()
      val matches    = metricsDF.filter($"match").count()
      val accuracy   = if (totalProps == 0) 0.0 else matches.toDouble / totalProps
      val avgDelta   = metricsDF.agg(avg($"delta").as("avg")).collect()(0).getAs[Double]("avg")

      println(s"\n=== Type Inference Metrics for $name ===")
      println(f"- Properties compared: $totalProps%d")
      println(f"- Type agreement (sample vs full): ${accuracy * 100}%.2f%%")
      println(f"- Avg support delta (|sample-full|): ${avgDelta * 100}%.2f%%")

      val d = metricsDF.select(abs(col("delta")).as("delta"))

      val binsRow = d.agg(
        sum(when(col("delta") < 0.05, 1).otherwise(0)).as("b1"),
        sum(when(col("delta") >= 0.05 && col("delta") < 0.10, 1).otherwise(0)).as("b2"),
        sum(when(col("delta") >= 0.10 && col("delta") < 0.20, 1).otherwise(0)).as("b3"),
        sum(when(col("delta") >= 0.20 && col("delta") < 0.30, 1).otherwise(0)).as("b4"),
        sum(when(col("delta") >= 0.30, 1).otherwise(0)).as("b5")
      ).first()

      val (b1,b2,b3,b4,b5) = (
        binsRow.getLong(0),
        binsRow.getLong(1),
        binsRow.getLong(2),
        binsRow.getLong(3),
        binsRow.getLong(4)
      )

      println("\nDelta histogram bins (counts):")
      println(f"- [0,0.05)   : $b1%d")
      println(f"- [0.05,0.10): $b2%d")
      println(f"- [0.10,0.20): $b3%d")
      println(f"- [0.20,0.30): $b4%d")
      println(f"- ≥0.30       : $b5%d")

      println(s"\nHistogram coordinates: ([0,0.05), $b1) ([0.05,0.10), $b2) ([0.10,0.20), $b3) ([0.20,0.30), $b4) (≥0.30, $b5)")

      // Προαιρετικά: Top διαφορές
      val mismatches = metricsDF.filter(!$"match")
        .orderBy(col("delta").desc)
        .select("property","sample_type","full_type","sample_support","full_support","delta")

      if (mismatches.count() > 0) {
        println("\nTop differing properties by |delta|:")
        mismatches.show(50, truncate = false)
      }

    } else {
      allProperties.foreach { prop =>
        val sampleDF = originalDF.filter(col(prop).isNotNull).limit(1000).cache()
        val sampleCount = sampleDF.count()
        println(s"Sampling $prop: $sampleCount rows with non-null values")

        val values = sampleDF.select(prop).collect().map(_.getString(0)).toSeq

        val inferredType =
          if (values.isEmpty) "Unknown (empty)"
          else {
            val allIntegers = values.forall(isInteger)
            val allDoubles  = values.forall(isDouble)
            val allDates    = values.forall(isDate)
            val allBooleans = values.forall(isBoolean)

            if (prop == "locationIP" || values.forall(isIP)) "String"
            else if (allIntegers) "Integer"
            else if (allDoubles)  "Double"
            else if (allDates)    "Date"
            else if (allBooleans) "Boolean"
            else "String"
          }

        inferredTypes(prop) = inferredType
        sampleDF.unpersist()
      }

      println(s"\nInferred Property Types for $name (from sample):")
      inferredTypes.foreach { case (p,t) => println(s"Property: $p, Inferred Type: $t") }
    }

    val typeMapUdf = udf((properties: Seq[String]) =>
      if (properties == null || properties.isEmpty) Seq.empty[String]
      else properties.map { prop =>
        val base = if (prop.startsWith("prop_")) prop.stripPrefix("prop_") else prop
        s"$prop:${inferredTypes.getOrElse(base, "Unknown")}"
      }
    )

    val updatedDF = propertyCols.foldLeft(mergedDF) { (df, colName) =>
      df.schema(colName).dataType match {
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

    // src->dst cardinality
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