import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Word2Vec, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors

object PatternPreprocessing {

  def encodePatterns(spark: SparkSession,
                     patternsDF: DataFrame,
                     allProperties: Set[String]): DataFrame = {

    import spark.implicits._
    val patternsDFwithArray = patternsDF.withColumn("propertiesArray", $"properties")

    val withBinaryColsDF = allProperties.foldLeft(patternsDFwithArray) { (tempDF, prop) =>
      tempDF.withColumn(
        s"prop_$prop",
        when(array_contains($"propertiesArray", prop), 1.0).otherwise(0.0)
      )
    }

    val withLabelArrDF = withBinaryColsDF.withColumn(
      "labelArray",
      array($"label")
    )

    val word2Vec = new Word2Vec()
      .setInputCol("labelArray")
      .setOutputCol("labelVector")
      .setVectorSize(3)
      .setMinCount(0)

    val w2vModel = word2Vec.fit(withLabelArrDF)
    val withLabelVecDF = w2vModel.transform(withLabelArrDF)

    val binaryCols = allProperties.toArray.map(p => s"prop_$p")
    val assembler = new VectorAssembler()
      .setInputCols(Array("labelVector") ++ binaryCols)
      .setOutputCol("features")

    val finalDF = assembler.transform(withLabelVecDF)
    finalDF.show(50, truncate = false)

    finalDF
  }

  def encodeEdgePatterns(spark: SparkSession,
                         edgesDF: DataFrame,
                         allProperties: Set[String]): DataFrame = {
    import spark.implicits._

    val withArrayDF = edgesDF.withColumn("propertiesArray", $"properties")

    val binDF = allProperties.foldLeft(withArrayDF) { (tempDF, prop) =>
      tempDF.withColumn(
        s"prop_$prop",
        when(array_contains($"propertiesArray", prop), 1.0).otherwise(0.0)
      )
    }

    val withArrays = binDF
      .withColumn("relationArray", array($"relationshipType"))
      .withColumn("srcLabelArray", array($"srcLabel"))
      .withColumn("dstLabelArray", array($"dstLabel"))

    val word2VecRel = new org.apache.spark.ml.feature.Word2Vec()
      .setInputCol("relationArray")
      .setOutputCol("relVector")
      .setVectorSize(3)
      .setMinCount(0)

    val relModel = word2VecRel.fit(withArrays)
    val withRelVec = relModel.transform(withArrays)

    val word2VecSrc = new org.apache.spark.ml.feature.Word2Vec()
      .setInputCol("srcLabelArray")
      .setOutputCol("srcVector")
      .setVectorSize(3)
      .setMinCount(0)

    val srcModel = word2VecSrc.fit(withRelVec)
    val withSrcVec = srcModel.transform(withRelVec)

    val word2VecDst = new org.apache.spark.ml.feature.Word2Vec()
      .setInputCol("dstLabelArray")
      .setOutputCol("dstVector")
      .setVectorSize(3)
      .setMinCount(0)

    val dstModel = word2VecDst.fit(withSrcVec)
    val withDstVec = dstModel.transform(withSrcVec)

    val propCols = allProperties.toArray.sorted.map(p => s"prop_$p")

    val assembler = new org.apache.spark.ml.feature.VectorAssembler()
      .setInputCols(Array("relVector", "srcVector", "dstVector") ++ propCols)
      .setOutputCol("features")

    val finalDF = assembler.transform(withDstVec)

    finalDF
  }
}
