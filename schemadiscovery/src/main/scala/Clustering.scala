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


  /**
    * Computes the LSH Jaccard similarity pairs.
    */
  def LSHClusteringNodes(
      nodesDF: DataFrame,
      similarityThreshold: Double = 0.8,
      desiredCollisionProbability: Double = 0.9,
      distanceCutoff: Double = 0.2,
      datasetSize: Int
    )
    (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val propertyColumns = nodesDF.columns.filterNot(colName => 
      Seq("_nodeId", "features", "knownLabels").contains(colName)
    )


    val assembler = new VectorAssembler()
      .setInputCols(nodesDF.columns.filterNot(colName => colName == "_nodeId" || colName == "knownLabels"))
      .setOutputCol("features")

    val featureDF = assembler.transform(nodesDF)

    val numHashTables = calculateNumHashTables(datasetSize)

    println(s"Adaptive numHashTables: $numHashTables")

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0) // Adjust based on dataset
      .setNumHashTables(numHashTables) // Try different values
      .setInputCol("features")
      .setOutputCol("hashes")
      
      val brpModel = brp.fit(featureDF)
      val transformedDF = brpModel.transform(featureDF)



    val lshClean = transformedDF.withColumnRenamed("knownLabels", "lshKnownLabels")

    val lshWithLabels = lshClean.join(
      nodesDF.select("_nodeId", "knownLabels"),
      Seq("_nodeId"),
      joinType = "left" 
    ) // this is for the knownLabels

    val distinctHashPatterns = lshWithLabels
      .groupBy($"hashes")
      .agg(
        collect_list($"_nodeId").as("nodesWithThisHash"),
        collect_list($"knownLabels").as("labelsForNodes"),
        count($"_nodeId").as("countNodes")
      )
      .orderBy(desc("countNodes"))


    // println("\n---- countNodes for first 10 distinct hash signatures ----")
    // distinctHashPatterns.select("countNodes").show(10, truncate = false)
    val distinctCluster = extractDistinctLabels(distinctHashPatterns)(spark)
    distinctCluster
  }

  def LSHClusteringEdges(
    edgesDF: DataFrame,
    similarityThreshold: Double = 0.8,
    desiredCollisionProbability: Double = 0.9,
    distanceCutoff: Double = 0.2,
    datasetSize: Int
  )
  (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Assemble features (excluding srcId, dstId, relationshipType, knownRelationships, etc.)
    val assembler = new VectorAssembler()
      .setInputCols(
        edgesDF.columns.filterNot { colName =>
          colName == "srcId" || 
          colName == "dstId" ||
          colName == "knownRelationships" || 
          colName == "srcType" ||
          colName == "dstType"
        }
      )
      .setOutputCol("features")

    val featureDF = assembler.transform(edgesDF)
    val numHashTables = calculateNumHashTables(datasetSize)

    println(s"Adaptive numHashTables: $numHashTables")
    
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0) // Adjust based on dataset
      .setNumHashTables(numHashTables) // Try different values
      .setInputCol("features")
      .setOutputCol("hashes")
      
    val brpModel = brp.fit(featureDF)
    val transformedDF = brpModel.transform(featureDF)
    // println("\n---- Sample of LSH output (hashes) ----")
    // transformedDF.show(5, truncate = false)

    // Keep your edges’ knownRelationships as lshKnownRelationships for clarity
    val lshClean = transformedDF.withColumnRenamed("knownRelationships", "lshKnownRelationships")

    // Join to bring back any additional columns (like relationshipType)
    val lshWithLabels = lshClean.join(
      edgesDF.select("srcId", "dstId", "relationshipType"),
      Seq("srcId", "dstId", "relationshipType"),
      joinType = "left"
    )

    val distinctHashPatterns = lshWithLabels
        .groupBy($"hashes")
        .agg(
          collect_set($"lshKnownRelationships").as("labelsForEdges"),
          collect_set($"srcType").as("srcTypesForEdges"),
          collect_set($"dstType").as("dstTypesForEdges")
        )

      distinctHashPatterns
        .select("labelsForEdges", "srcTypesForEdges", "dstTypesForEdges")
        .show(truncate = false)

    distinctHashPatterns
  }


}