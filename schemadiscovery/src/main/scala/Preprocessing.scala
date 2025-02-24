import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{MinHashLSH, VectorAssembler, Word2Vec}
object Preprocessing {

  /**
    * Creates a binary matrix (0-1) from the input DataFrame.
    * Keeps the original labels before removal.
    */
  def createBinaryMatrixforNodes(df: DataFrame): DataFrame = {
    val spark = df.sparkSession

    val propertyColumns = df.columns.filterNot(colName => colName == "_nodeId" || colName == "knownLabels")

    // Transform each property column to binary (1 if not null, 0 otherwise)
    val binaryDF = propertyColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

    // Apply Word2Vec for labels
    val word2Vec = new Word2Vec()
      .setInputCol("knownLabels")
      .setOutputCol("word2vec")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(binaryDF)
    val result = model.transform(binaryDF)

    println("Sample data from Word2Vec for Nodes:")
    result.show(50)

    result
  }

  def createBinaryMatrixforEdges(df: DataFrame): DataFrame = {
    val spark = df.sparkSession

    val propertyColumns = df.columns.filterNot(colName =>
      Seq("srcId", "dstId", "knownRelationships", "srcType", "dstType").contains(colName)
    )

    // Transform each property column to binary (1 if not null, 0 otherwise)
    val binaryDF = propertyColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

    // Word2Vec embeddings for relationships
    val word2Vec = new Word2Vec()
      .setInputCol("knownRelationships")
      .setOutputCol("word2vecRel")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(binaryDF)
    val result = model.transform(binaryDF)

    // Word2Vec embeddings for srcType
    val word2Vec2 = new Word2Vec()
      .setInputCol("srcType")
      .setOutputCol("word2vecSrc")
      .setVectorSize(3)
      .setMinCount(0)

    val model2 = word2Vec2.fit(result)
    val result2 = model2.transform(result)

    // Word2Vec embeddings for dstType
    val word2Vec3 = new Word2Vec()
      .setInputCol("dstType")
      .setOutputCol("word2vecDst")
      .setVectorSize(3)
      .setMinCount(0)

    val model3 = word2Vec3.fit(result2)
    val result3 = model3.transform(result2)

    println("Sample data from Word2Vec for Edges:")
    result3.show(5)

    result3
  }

  /**
    * Preprocessing pipeline: keeps original labels before removal and creates a binary matrix.
    */
  def preprocessing(spark: SparkSession,
                    nodesDF: DataFrame,
                    edgesDF: DataFrame,
                    dropProbability: Double): (DataFrame, DataFrame) = {

    import spark.implicits._

    // Store original labels before removal
    val nodesWithKnown = nodesDF
      .withColumn("originalLabels", split(col("_labels"), ":")) // Keep original labels
      .withColumn("knownLabels",
        when(rand(123) < dropProbability, typedLit(Seq.empty[String])) // Drop with probability
          .otherwise(col("originalLabels"))
      )

    val edgesWithKnown = edgesDF
      .withColumn("originalRelationships", split(col("relationshipType"), ":")) // Keep original relationships
      .withColumn("srcType", split(col("srcType"), ":")) // Keep original srcType
      .withColumn("dstType", split(col("dstType"), ":")) // Keep original dstType
      .withColumn("knownRelationships",
        when(rand(123) < dropProbability, typedLit(Seq.empty[String]))
          .otherwise(col("originalRelationships"))
      )

    // Convert to binary matrix
    val binaryMatrixforNodesDF_LSH = createBinaryMatrixforNodes(nodesWithKnown).cache()
    val binaryMatrixforEdgesDF_LSH = createBinaryMatrixforEdges(edgesWithKnown).cache()

    println("Binary Matrix for Nodes:")
    binaryMatrixforNodesDF_LSH.show(5)

    println("Binary Matrix for Edges:")
    binaryMatrixforEdgesDF_LSH.show(5)

    (binaryMatrixforNodesDF_LSH, binaryMatrixforEdgesDF_LSH)
  }
}
