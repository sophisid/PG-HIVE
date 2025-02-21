import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{MinHashLSH, VectorAssembler, Word2Vec, BucketedRandomProjectionLSH}
import org.apache.spark.ml.linalg.{Vector, Vectors, SparseVector, DenseVector}

object Clustering {

  // UDF for majority vote
  val majorityVoteUDF = udf { (labelsSeq: Seq[Seq[String]]) =>
    val flat = labelsSeq.flatten
    if (flat.isEmpty) null else flat.groupBy(identity).mapValues(_.size).maxBy(_._2)._1
  }

  val binarizeVector = udf { vector: Vector =>
    vector match {
      case v: SparseVector => Vectors.sparse(v.size, v.indices, Array.fill(v.indices.length)(1.0))
      case v: DenseVector  => Vectors.dense(v.values.map(x => if (x != 0.0) 1.0 else 0.0))
    }
  }

  // to cite Indyk & Motwani (1998) – Approximate Nearest Neighbors: Towards Removing the Curse of Dimensionality
  // Gionis et al. (1999) – Similarity Search in High Dimensions via Hashing pages = {518--529}

  def calculateNumHashTables(datasetSize: Int, scalingFactor: Double = 15.0): Int = {
    require(datasetSize > 0, "Dataset size must be positive.")

    val L = scalingFactor * math.log(datasetSize)
    scala.math.ceil(L).toInt
  }

  def extractDistinctLabels(distinctHashPatterns: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val distinctLabelsPerCluster = distinctHashPatterns
      .withColumn("labelsForNodes", explode($"labelsForNodes")) // Flatten the label list
      .filter($"labelsForNodes".isNotNull) // Remove nulls
      .groupBy($"hashes")
      .agg(
        countDistinct($"labelsForNodes").as("uniqueLabelCount"),
        collect_set($"labelsForNodes").as("distinctLabels")
      )

    // Print only the required columns
    println("\n---- Unique Labels per Cluster ----")
    distinctLabelsPerCluster.select("uniqueLabelCount", "distinctLabels").show(truncate = false)

    distinctLabelsPerCluster
  }

  val findPatternColumnsUDF = udf { (features: Vector, colNames: Seq[String]) =>
    features.toArray.zip(colNames).collect { case (value, name) if value == 1.0 => name }
  }


  /**
    * Computes the LSH Jaccard similarity pairs.
    */
  def LSHClusteringNodes(
      nodesDF: DataFrame,
      similarityThreshold: Double = 0.8,
      desiredCollisionProbability: Double = 0.9,
      distanceCutoff: Double = 0.2,
      datasetSize: Int
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Identify the property columns (excluding metadata)
    val propertyColumns = nodesDF.columns.filterNot(colName =>
      Seq("_nodeId", "features", "knownLabels").contains(colName)
    )

    // UDF to extract the columns with value 1 (active properties)
    val findPatternColumnsUDF = udf { (features: Vector, colNames: Seq[String]) =>
      features.toArray.zip(colNames).collect { case (value, name) if value == 1.0 => name }
    }

    // Convert properties into a feature vector
    val assembler = new VectorAssembler()
      .setInputCols(propertyColumns)
      .setOutputCol("features")

    val featureDF = assembler.transform(nodesDF)

    val numHashTables = calculateNumHashTables(datasetSize)
    println(s"Adaptive numHashTables: $numHashTables")

    // Apply LSH for clustering
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")

    val brpModel = brp.fit(featureDF)
    val transformedDF = brpModel.transform(featureDF)

    // Extract active features for each node
    val withPatterns = transformedDF.withColumn(
      "patternColumns",
      findPatternColumnsUDF($"features", typedLit(propertyColumns))
    )

    // Group by cluster (hashes) and collect distinct patterns
    val discoveredPatterns = withPatterns
      .groupBy($"hashes")
      .agg(
        collect_set($"knownLabels").as("EntityType"),
        collect_set($"patternColumns").as("Patterns")
      )

    // Print discovered patterns in a structured format
    println("\n---- Discovered Patterns ----")
    discoveredPatterns.collect().foreach { row =>
      val entityTypes = row.getAs[Seq[Seq[String]]]("EntityType").flatten.distinct.mkString(", ")
      val patterns = row.getAs[Seq[Seq[String]]]("Patterns").flatten.distinct.mkString(", ")
      println(s"$entityTypes: $patterns")
    }

    discoveredPatterns
  }



def LSHClusteringEdges(
  edgesDF: DataFrame,
  similarityThreshold: Double = 0.8,
  desiredCollisionProbability: Double = 0.9,
  distanceCutoff: Double = 0.2,
  datasetSize: Int
)(implicit spark: SparkSession): DataFrame = {

  import spark.implicits._

  // Identify property columns (excluding metadata)
  val propertyColumns = edgesDF.columns.filterNot { colName =>
    Seq("srcId", "dstId", "knownRelationships", "srcType", "dstType").contains(colName)
  }

  // UDF to extract active features (columns with value 1)
  val findPatternColumnsUDF = udf { (features: Vector, colNames: Seq[String]) =>
    features.toArray.zip(colNames).collect { case (value, name) if value == 1.0 => name }
  }

  // Convert properties into a feature vector
  val assembler = new VectorAssembler()
    .setInputCols(propertyColumns)
    .setOutputCol("features")

  val featureDF = assembler.transform(edgesDF)

  val numHashTables = calculateNumHashTables(datasetSize)
  println(s"Adaptive numHashTables: $numHashTables")

  // Apply LSH for clustering
  val brp = new BucketedRandomProjectionLSH()
    .setBucketLength(2.0) // Adjust based on dataset
    .setNumHashTables(numHashTables)
    .setInputCol("features")
    .setOutputCol("hashes")

  val brpModel = brp.fit(featureDF)
  val transformedDF = brpModel.transform(featureDF)

  // Extract active features for each edge
  val withPatterns = transformedDF.withColumn(
    "patternColumns",
    findPatternColumnsUDF($"features", typedLit(propertyColumns))
  )

  // Group by relationship type (knownRelationships) and collect distinct patterns + range labels
  val discoveredPatterns = withPatterns
    .groupBy($"hashes")
    .agg(
      collect_set($"knownRelationships").as("RelationshipType"),  // ✅ Relationship type
      collect_set($"srcType").as("SourceType"),                  // ✅ Source node labels
      collect_set($"dstType").as("DestinationType"),             // ✅ Destination node labels
      collect_set($"patternColumns").as("Patterns")              // ✅ Features defining the relationship
    )

  // Print discovered patterns in a structured format
  println("\n---- Discovered Patterns for Relationships ----")
  discoveredPatterns.collect().foreach { row =>
    val relationshipTypes = row.getAs[Seq[Seq[String]]]("RelationshipType").flatten.distinct.mkString(", ")
    val sourceTypes = row.getAs[Seq[Seq[String]]]("SourceType").flatten.distinct.mkString(", ")
    val destinationTypes = row.getAs[Seq[Seq[String]]]("DestinationType").flatten.distinct.mkString(", ")
    val patterns = row.getAs[Seq[Seq[String]]]("Patterns").flatten.distinct.mkString(", ")

    println(s"$relationshipTypes ($sourceTypes → $destinationTypes): $patterns")
  }

  discoveredPatterns
}


}