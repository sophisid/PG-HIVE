import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{MinHashLSH, VectorAssembler, Word2Vec}

object Preprocessing {
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

    val propertyColumns = df.columns.filterNot(colName => colName == "srcId" || colName == "dstId" || colName == "knownRelationships" || colName == "srcType" || colName == "dstType")

    // Transform each property column to binary (1 if not null, 0 otherwise)
    val binaryDF = propertyColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

   //take the column of known labels and make it a word2vec
    val word2Vec = new Word2Vec()
      .setInputCol("knownRelationships")
      .setOutputCol("word2vecRel")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(binaryDF)
    val result = model.transform(binaryDF)

    val word2Vec2 = new Word2Vec()
      .setInputCol("srcType")
      .setOutputCol("word2vecSrc")
      .setVectorSize(3)
      .setMinCount(0)

    val model2 = word2Vec2.fit(result)
    val result2 = model2.transform(result)

    val word2Vec3 = new Word2Vec()
      .setInputCol("dstType")
      .setOutputCol("word2vecDst")
      .setVectorSize(3)
      .setMinCount(0)

    val model3 = word2Vec3.fit(result2)
    val result3 = model3.transform(result2)
    
    println("Sample data from word2vec:")
    result3.show(5)
    result3
  }

  def preprocessing(spark: SparkSession,
                    nodesDF: DataFrame,
                    edgesDF: DataFrame,
                    dropProbability: Double): (DataFrame, DataFrame) = {

    import spark.implicits._

    val nodesWithKnown = nodesDF.withColumn("knownLabels", split(col("_labels"), ":"))
    val nodesAfterRemoval = nodesWithKnown.withColumn(
      "knownLabels",
      when(rand(123) < dropProbability, typedLit(Seq.empty[String]))
        .otherwise(col("knownLabels"))
    )

    val edgesWithKnown = edgesDF
      .withColumn("knownRelationships", split(col("relationshipType"), ":"))
      .withColumn("srcType", split(col("srcType"), ":"))
      .withColumn("dstType", split(col("dstType"), ":"))

    val edgesAfterRemoval = edgesWithKnown
      .withColumn(
        "knownRelationships",
        when(rand(123) < dropProbability, typedLit(Seq.empty[String]))
          .otherwise(col("knownRelationships"))
      )

    val binaryMatrixforNodesDF_LSH = Preprocessing.createBinaryMatrixforNodes(nodesAfterRemoval).cache()
    val binaryMatrixforEdgesDF_LSH = Preprocessing.createBinaryMatrixforEdges(edgesAfterRemoval).cache()

    println("Binary Matrix for Nodes:")
    binaryMatrixforNodesDF_LSH.show(5)
    println("Binary Matrix for Edges:")
    binaryMatrixforEdgesDF_LSH.show(5)

    (binaryMatrixforNodesDF_LSH, binaryMatrixforEdgesDF_LSH)
  }


}
