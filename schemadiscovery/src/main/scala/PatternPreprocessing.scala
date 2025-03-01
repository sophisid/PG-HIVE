import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Word2Vec, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors

object PatternPreprocessing {

    def encodePatterns(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
        import spark.implicits._

        val propertyColumns = patternsDF.columns.filterNot(colName => Seq("patternId", "label").contains(colName))

        val binaryDF = propertyColumns.foldLeft(patternsDF) { (tempDF, colName) =>
        tempDF.withColumn(colName, when(col(colName).isNotNull, 1.0).otherwise(0.0))
        }

        val processedDF = binaryDF.withColumn("labelArray", array(col("label")))

        val word2Vec = new Word2Vec()
        .setInputCol("labelArray")
        .setOutputCol("labelVector")
        .setVectorSize(3)
        .setMinCount(0)

        val model = word2Vec.fit(processedDF)
        val dfWithWord2Vec = model.transform(processedDF)

        val assembler = new VectorAssembler()
        .setInputCols(Array("labelVector") ++ propertyColumns)
        .setOutputCol("features")

        val finalDF = assembler.transform(dfWithWord2Vec)
        .drop("labelVector")

        println("Sample data after feature encoding for Nodes:")
        finalDF.show(50)

        finalDF
    }


  def encodeEdgePatterns(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
    import spark.implicits._

    val propertyColumns = patternsDF.columns.filterNot(colName =>
      Seq("patternId", "knownRelationships", "srcType", "dstType").contains(colName)
    )

    val binaryDF = propertyColumns.foldLeft(patternsDF) { (tempDF, colName) =>
      tempDF.withColumn(colName, when(col(colName).isNotNull, 1).otherwise(0))
    }

    val processedDF = binaryDF
      .withColumn("knownRelationshipsArray", split(col("knownRelationships"), ":"))
      .withColumn("srcTypeArray", split(col("srcType"), ":"))
      .withColumn("dstTypeArray", split(col("dstType"), ":"))

    val word2VecRel = new Word2Vec()
      .setInputCol("knownRelationshipsArray")
      .setOutputCol("word2vecRel")
      .setVectorSize(3)
      .setMinCount(0)

    val modelRel = word2VecRel.fit(processedDF)
    val dfWithRelVec = modelRel.transform(processedDF)

    val word2VecSrc = new Word2Vec()
      .setInputCol("srcTypeArray")
      .setOutputCol("word2vecSrc")
      .setVectorSize(3)
      .setMinCount(0)

    val modelSrc = word2VecSrc.fit(dfWithRelVec)
    val dfWithSrcVec = modelSrc.transform(dfWithRelVec)

    val word2VecDst = new Word2Vec()
      .setInputCol("dstTypeArray")
      .setOutputCol("word2vecDst")
      .setVectorSize(3)
      .setMinCount(0)

    val modelDst = word2VecDst.fit(dfWithSrcVec)
    val result = modelDst.transform(dfWithSrcVec)

    println("Sample data from Word2Vec for Edges:")
    result.show(5)

    result
  }
}
