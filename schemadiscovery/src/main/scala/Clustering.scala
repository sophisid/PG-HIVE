import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{MinHashLSH, VectorAssembler, Word2Vec}
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

  def calculateNumHashTables(datasetSize: Int, scalingFactor: Double = 10.0): Int = {
    require(datasetSize > 0, "Dataset size must be positive.")

    val L = scalingFactor * math.log(datasetSize)
    scala.math.ceil(L).toInt
  }

  def calculateNumHashFunctions(datasetSize: Int, similarityThreshold: Double): Int = {
    require(datasetSize > 0, "Dataset size must be positive.")
    require(similarityThreshold > 0.0 && similarityThreshold < 1.0, 
            "Similarity threshold must be between 0 and 1 (exclusive).")

    val k = math.log(datasetSize) / math.log(1.0 / similarityThreshold)
    scala.math.ceil(k).toInt
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
    //take the column of known labels and make it a word2vec
    val word2Vec = new Word2Vec()
      .setInputCol("knownLabels")
      .setOutputCol("word2vec")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(binaryDF)
    val result = model.transform(binaryDF)

    println("Sample data from word2vec:")
    result.show(5)
    result
  }

  def createBinaryMatrixforEdges(df: DataFrame): DataFrame = {
    val spark = df.sparkSession

    val propertyColumns = df.columns.filterNot(colName => colName == "srcId" || colName == "dstId" || colName == "knownRelationships")

    // Transform each property column to binary (1 if not null, 0 otherwise)
    val binaryDF = propertyColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

   //take the column of known labels and make it a word2vec
    val word2Vec = new Word2Vec()
      .setInputCol("knownRelationships")
      .setOutputCol("word2vec")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(binaryDF)
    val result = model.transform(binaryDF)
    
    println("Sample data from word2vec:")
    result.show(5)
    result
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

    val assembler = new VectorAssembler()
      .setInputCols(nodesDF.columns.filterNot(colName => colName == "_nodeId" || colName == "knownLabels"))
      .setOutputCol("features")

    val featureDF = assembler.transform(nodesDF)

    val numHashTables = calculateNumHashTables(datasetSize)
    val numHashFunctions = calculateNumHashFunctions(datasetSize, similarityThreshold)

    println(s"Adaptive numHashTables: $numHashTables")
    println(s"Adaptive numHashFunctions: $numHashFunctions")

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
    // distinctHashPatterns.show(20, truncate = false)

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
      distanceCutoff: Double = 0.2,
      datasetSize: Int
    )
    (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val assembler = new VectorAssembler()
      .setInputCols(edgesDF.columns.filterNot(colName => colName == "srcId" || colName == "dstId" || colName == "knownRelationships"))
      .setOutputCol("features")

    val featureDF = assembler.transform(edgesDF)

    // find the number of hash tables to use
    val numHashTables = calculateNumHashTables(datasetSize)
    val numHashFunctions = calculateNumHashFunctions(datasetSize, similarityThreshold)

    println(s"Adaptive numHashTables: $numHashTables")
    println(s"Adaptive numHashFunctions: $numHashFunctions")

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
    // distinctHashPatterns.show(20, truncate = false)

    println("\n---- countEdges for first 10 distinct hash signatures ----")
    distinctHashPatterns.select("countEdges").show(10, truncate = false)

    distinctHashPatterns
  }

  

}