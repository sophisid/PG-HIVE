import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.linalg.{Vector, Vectors}
import scala.math.sqrt

object LSHClustering {

  def estimateLSHParams(df: DataFrame, featuresCol: String, sampleSize: Int = 1000): (Double, Int) = {
    import df.sparkSession.implicits._

    val sampledDF = df.sample(false, sampleSize.toDouble / df.count(), 42).limit(sampleSize).cache()
    println(s"Sampled ${sampledDF.count()} rows for LSH parameter estimation")

    val featurePairs = sampledDF.crossJoin(sampledDF.withColumnRenamed(featuresCol, "features2"))
      .select(col(featuresCol).as("f1"), col("features2").as("f2"))
      .filter(col("f1") =!= col("f2"))
      .limit(10000)

    val distanceUdf = udf((v1: Vector, v2: Vector) => {
      val diff = v1.toArray.zip(v2.toArray).map { case (x, y) => (x - y) * (x - y) }
      sqrt(diff.sum)
    })

    val distances = featurePairs
      .withColumn("distance", distanceUdf(col("f1"), col("f2")))
      .agg(
        avg("distance").as("avgDistance"),
        min("distance").as("minDistance"),
        max("distance").as("maxDistance"),
        stddev("distance").as("stddevDistance")
      )
      .collect()(0)

    val avgDistance = distances.getAs[Double]("avgDistance")
    val minDistance = distances.getAs[Double]("minDistance")
    val maxDistance = distances.getAs[Double]("maxDistance")
    val stddevDistance = distances.getAs[Double]("stddevDistance")

    println(s"Avg Distance: $avgDistance, Min Distance: $minDistance, Max Distance: $maxDistance, StdDev: $stddevDistance")

    val bucketLength = avgDistance * 0.1
    println(s"Estimated bucketLength: $bucketLength")

    val totalRows = df.count()
    val dimensionality = sampledDF.select(featuresCol).head().getAs[Vector](0).size
    val numHashTables = math.min(10, math.max(5, (totalRows / 10000).toInt))
    println(s"Estimated numHashTables: $numHashTables (based on $totalRows rows and $dimensionality dimensions)")

    sampledDF.unpersist()

    (bucketLength, numHashTables)
  }

  def applyLSHNodes(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
    import spark.implicits._

    if (patternsDF.isEmpty) {
      println("No patterns to cluster.")
      return spark.emptyDataFrame
    }

    val (bucketLength, numHashTables) = estimateLSHParams(patternsDF, "features")
    
    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(bucketLength)
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(patternsDF)
    val transformedDF = model.transform(patternsDF)

    val propCols = patternsDF.columns.filterNot(Seq("_nodeId", "_labels", "originalLabels").contains)
    val nonNullProps = array(
      propCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))).toSeq: _*
    ).as("nonNullProps")

    val clusteredDF = transformedDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"hashes")
      .agg(
        collect_set($"_labels").as("labelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propertiesInCluster"),
        collect_list($"_nodeId").as("nodeIdsInCluster")
      )
      .withColumn("row_num", row_number().over(Window.orderBy($"hashes")))
      .withColumn("cluster_id", concat(lit("cluster_node_"), $"row_num"))
      .drop("hashes", "row_num")

    clusteredDF
  }

  def applyLSHEdges(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    if (df.isEmpty) {
      println("No edge patterns to cluster.")
      return spark.emptyDataFrame
    }

    val (bucketLength, numHashTables) = estimateLSHParams(df, "features")

    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(bucketLength)
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(df)
    val transformedDF = model.transform(df)

    val propCols = df.columns.filter(_.startsWith("prop_"))
    val nonNullProps = array(
      propCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))).toSeq: _*
    ).as("nonNullProps")

    val groupedDF = transformedDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"hashes")
      .agg(
        collect_set($"relationshipType").as("relsInCluster"),
        collect_set($"srcType").as("srcLabelsInCluster"),
        collect_set($"dstType").as("dstLabelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propsInCluster"),
        collect_list(struct($"srcId", $"dstId")).as("edgeIdsInCluster")
      )
      .withColumn("row_num", row_number().over(Window.orderBy($"hashes")))
      .withColumn("cluster_id", concat(lit("cluster_edge_"), $"row_num"))
      .drop("hashes", "row_num")

    groupedDF
  }

  def mergePatternsByLabel(spark: SparkSession, clusteredNodes: DataFrame): DataFrame = {
    import spark.implicits._

    clusteredNodes.cache()

    val withLabelsDF = clusteredNodes.filter(size($"labelsInCluster") > 0)
    val noLabelsDF = clusteredNodes.filter(size($"labelsInCluster") === 0)

    println(s"Number of node clusters before merge: ${clusteredNodes.count()}")

    val mergedWLabelDF = withLabelsDF
      .withColumn("sortedLabels", array_sort($"labelsInCluster"))
      .groupBy($"sortedLabels")
      .agg(
        collect_list($"propertiesInCluster").as("propertiesInCluster"),
        flatten(collect_list($"nodeIdsInCluster")).as("nodeIdsInCluster"),
        collect_set($"cluster_id").as("original_cluster_ids")
      )

    val finalDF = mergedWLabelDF
      .withColumn("mandatoryProperties",
        flatten(aggregate(
          $"propertiesInCluster",
          $"propertiesInCluster"(0),
          (acc, props) => array_intersect(acc, props)
        ))
      )
      .withColumn("allProperties", flatten(flatten($"propertiesInCluster")))
      .withColumn("optionalProperties",
        array_distinct(array_except($"allProperties", $"mandatoryProperties"))
      )
      .drop("allProperties")
      .withColumn("row_num", row_number().over(Window.orderBy($"sortedLabels")))
      .withColumn("merged_cluster_id", concat(lit("merged_cluster_node_"), $"row_num"))
      .drop("row_num")

    val noLabelsFinalDF = noLabelsDF
      .select(
        $"labelsInCluster".as("sortedLabels"),
        array($"propertiesInCluster").as("propertiesInCluster"),
        $"nodeIdsInCluster",
        flatten($"propertiesInCluster").as("mandatoryProperties"),
        array().cast("array<string>").as("optionalProperties"),
        array($"cluster_id").as("original_cluster_ids"),
        $"cluster_id".as("merged_cluster_id")
      )

    println("Schema of finalDF:")
    finalDF.printSchema()
    println("Schema of noLabelsFinalDF:")
    noLabelsFinalDF.printSchema()

    val returnedDF = finalDF.union(noLabelsFinalDF)
    returnedDF
  }

  def mergeEdgePatternsByLabel(spark: SparkSession, clusteredEdges: DataFrame): DataFrame = {
    import spark.implicits._

    clusteredEdges.cache()

    val withLabelsDF = clusteredEdges.filter(
      (size($"relsInCluster") > 0) &&
      (size($"srcLabelsInCluster") > 0 || size($"dstLabelsInCluster") > 0)
    )
    val noLabelsDF = clusteredEdges.filter(
      (size($"relsInCluster") > 0) &&
      (size($"srcLabelsInCluster") === 0 && size($"dstLabelsInCluster") === 0)
    )

    println(s"Number of edge clusters before merge: ${clusteredEdges.count()}")

    val mergedDF = withLabelsDF
      .withColumn("sortedRelationshipTypes", array_sort($"relsInCluster"))
      .withColumn("sortedSrcLabels", array_sort($"srcLabelsInCluster"))
      .withColumn("sortedDstLabels", array_sort($"dstLabelsInCluster"))
      .groupBy($"sortedRelationshipTypes", $"sortedSrcLabels", $"sortedDstLabels")
      .agg(
        collect_list($"propsInCluster").as("propsNested"),
        flatten(collect_list($"edgeIdsInCluster")).as("edgeIdsInCluster"),
        collect_set($"cluster_id").as("original_cluster_ids")
      )

    val finalDF = mergedDF
      .withColumn("propsInCluster", flatten($"propsNested"))
      .withColumn("mandatoryProperties",
        aggregate(
          $"propsInCluster",
          $"propsInCluster"(0),
          (acc, props) => array_intersect(acc, props)
        )
      )
      .withColumn("flattenedProps", flatten($"propsInCluster"))
      .withColumn("optionalProperties",
        array_distinct(array_except($"flattenedProps", $"mandatoryProperties"))
      )
      .withColumn("row_num", row_number().over(Window.orderBy($"sortedRelationshipTypes", $"sortedSrcLabels", $"sortedDstLabels")))
      .withColumn("merged_cluster_id", concat(lit("merged_cluster_edge_"), $"row_num"))
      .select(
        $"sortedRelationshipTypes".as("relationshipTypes"),
        $"sortedSrcLabels".as("srcLabels"),
        $"sortedDstLabels".as("dstLabels"),
        $"propsInCluster",
        $"edgeIdsInCluster",
        $"mandatoryProperties",
        $"optionalProperties",
        $"original_cluster_ids",
        $"merged_cluster_id"
      )
      .drop("flattenedProps", "row_num")

    val noLabelsFinalDF = noLabelsDF
      .select(
        $"relsInCluster".as("relationshipTypes"),
        $"srcLabelsInCluster".as("srcLabels"),
        $"dstLabelsInCluster".as("dstLabels"),
        $"propsInCluster",
        $"edgeIdsInCluster",
        flatten($"propsInCluster").as("mandatoryProperties"),
        array().cast("array<string>").as("optionalProperties"),
        array($"cluster_id").as("original_cluster_ids"),
        $"cluster_id".as("merged_cluster_id")
      )

    println("Schema of finalDF:")
    finalDF.printSchema()
    println("Schema of noLabelsFinalDF:")
    noLabelsFinalDF.printSchema()

    val returnedDF = finalDF.union(noLabelsFinalDF)
    returnedDF
  }
}