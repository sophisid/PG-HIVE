import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{MinHashLSH, VectorAssembler}
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

  def calculateNumHashTables(similarityThreshold: Double, desiredCollisionProbability: Double): Int = {
    require(similarityThreshold > 0.0 && similarityThreshold < 1.0,
      "similarityThreshold must be between 0 and 1 (exclusive).")
    require(desiredCollisionProbability > 0.0 && desiredCollisionProbability < 1.0,
      "desiredCollisionProbability must be between 0 and 1 (exclusive).")

    val numerator   = scala.math.log(1 - desiredCollisionProbability)
    val denominator = scala.math.log(similarityThreshold)
    val b = numerator / denominator

    scala.math.ceil(b).toInt 
  }

  /**
    * Creates a binary matrix 0-1 from the input DataFrame.
    */
  def createBinaryMatrixforNodes(df: DataFrame): DataFrame = {
    val spark = df.sparkSession

    val propertyColumns = df.columns.filterNot(colName => colName == "_nodeId" || colName == "knownLabels")

    // Transform each property column to binary (1 if not null, 0 otherwise)
    val binaryDF = propertyColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

    println(s"Binary matrix created with ${binaryDF.columns.length} columns.")
    println("Sample data from binary matrix:")
    binaryDF.show(5)
    binaryDF
  }

  def createBinaryMatrixforEdges(df: DataFrame): DataFrame = {
    val spark = df.sparkSession

    val propertyColumns = df.columns.filterNot(colName => colName == "srcId" || colName == "dstId" || colName == "relationshipType")

    // Transform each property column to binary (1 if not null, 0 otherwise)
    val binaryDF = propertyColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

    println(s"Binary matrix created with ${binaryDF.columns.length} columns.")
    println("Sample data from binary matrix:")
    binaryDF.show(5)
    binaryDF
  }

  /**
    * Computes the LSH Jaccard similarity pairs.
    */
  def LSHClusteringNodes(
      nodesDF: DataFrame,
      similarityThreshold: Double = 0.8,
      desiredCollisionProbability: Double = 0.9,
      distanceCutoff: Double = 0.2
    )
    (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val assembler = new VectorAssembler()
      .setInputCols(nodesDF.columns.filterNot(colName => colName == "_nodeId" || colName == "knownLabels"))
      .setOutputCol("features")

    val featureDF = assembler.transform(nodesDF)

    // find the number of hash tables to use
    //TODO needs refinement
    val numHashTables = calculateNumHashTables(similarityThreshold, desiredCollisionProbability)
    println(s"Using numHashTables: $numHashTables")

    val mh = new MinHashLSH()
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")
      .setSeed(12345L)

    val mhModel = mh.fit(featureDF)
    val lshDF   = mhModel.transform(featureDF)

    println("\n---- Sample of LSH output (hashes) ----")
    lshDF.show(5, truncate = false)


    val lshClean = lshDF.withColumnRenamed("knownLabels", "lshKnownLabels")

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

    println("\n---- Distinct Hash Patterns with Labels ----")
    distinctHashPatterns.show(20, truncate = false)

    println("\n---- countNodes for first 10 distinct hash signatures ----")
    distinctHashPatterns.select("countNodes").show(10, truncate = false)


    /*
    val similarPairs = mhModel
      .approxSimilarityJoin(lshDF, lshDF, distanceCutoff, "JaccardDistance")
      .filter("datasetA._nodeId != datasetB._nodeId")
      .select(
        $"datasetA._nodeId".alias("nodeA"),
        $"datasetB._nodeId".alias("nodeB"),
        $"JaccardDistance"
      )

    println("---- Similar Pairs (nodeA, nodeB, JaccardDistance) ----")
    similarPairs.show(20, truncate = false)

    similarPairs
    */

    // (hash->nodes->labels).
    distinctHashPatterns
  }

  def LSHClusteringEdges(
      edgesDF: DataFrame,
      similarityThreshold: Double = 0.8,
      desiredCollisionProbability: Double = 0.9,
      distanceCutoff: Double = 0.2
    )
    (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val assembler = new VectorAssembler()
      .setInputCols(edgesDF.columns.filterNot(colName => colName == "srcId" || colName == "dstId" || colName == "relationshipType"))
      .setOutputCol("features")

    val featureDF = assembler.transform(edgesDF)

    // find the number of hash tables to use
    //TODO needs refinement
    val numHashTables = calculateNumHashTables(similarityThreshold, desiredCollisionProbability)
    println(s"Using numHashTables: $numHashTables")

    val mh = new MinHashLSH()
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")
      .setSeed(12345L)

    val mhModel = mh.fit(featureDF)
    val lshDF   = mhModel.transform(featureDF)

    println("\n---- Sample of LSH output (hashes) ----")
    lshDF.show(5,truncate = false)

    
    val lshClean = lshDF.withColumnRenamed("knownRelationships", "lshKnownRelationships")

    val lshWithLabels = lshClean.join(
      edgesDF.select("srcId", "dstId", "relationshipType"),
      Seq("srcId", "dstId", "relationshipType"),
      joinType = "left" 
    ) // this is for the knownLabels

    val distinctHashPatterns = lshWithLabels
      .groupBy($"hashes")
      .agg(
        collect_list($"srcId").as("srcIdForThisHash"),
        collect_list($"dstId").as("dstIdForThisHash"),
        collect_list($"relationshipType").as("relationshipTypeForThisHash"),
        count($"srcId").as("countEdges")
      )
      .orderBy(desc("countEdges"))

    println("\n---- Distinct Hash Patterns with Labels ----")
    distinctHashPatterns.show(20, truncate = false)

    println("\n---- countEdges for first 10 distinct hash signatures ----")
    distinctHashPatterns.select("countEdges").show(10, truncate = false)

    distinctHashPatterns
  }

}