import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Word2Vec, VectorAssembler}

object PatternPreprocessing {

  def encodePatterns(spark: SparkSession,
                    patternsDF: DataFrame,
                    allProperties: Set[String]): DataFrame = {
    import spark.implicits._

    // Convert Set[String] columns to arrays
    val patternsDFwithArray = patternsDF
      .withColumn("propertiesArray", $"properties")
      .withColumn("labelArray", array($"label".cast("string"))) // Convert Set[String] to array

    // Add binary columns for properties
    val withBinaryColsDF = allProperties.foldLeft(patternsDFwithArray) { (tempDF, prop) =>
      tempDF.withColumn(
        s"prop_$prop",
        when(array_contains($"propertiesArray", prop), 1.0).otherwise(0.0)
      )
    }

    // Apply Word2Vec to labelArray
    val word2Vec = new Word2Vec()
      .setInputCol("labelArray")
      .setOutputCol("labelVector")
      .setVectorSize(3)
      .setMinCount(0)

    val w2vModel = word2Vec.fit(withBinaryColsDF)
    val withLabelVecDF = w2vModel.transform(withBinaryColsDF)

    // Assemble features
    val binaryCols = allProperties.toArray.map(p => s"prop_$p")
    val assembler = new VectorAssembler()
      .setInputCols(Array("labelVector") ++ binaryCols)
      .setOutputCol("features")

    val finalDF = assembler.transform(withLabelVecDF)
    finalDF
  }

  def encodeEdgePatterns(spark: SparkSession,
                        edgesDF: DataFrame,
                        allProperties: Set[String]): DataFrame = {
    import spark.implicits._

    // Convert Set[String] columns to arrays
    val withArrayDF = edgesDF
      .withColumn("propertiesArray", $"properties")
      .withColumn("relationshipTypeArray", array($"relationshipType".cast("string")))
      .withColumn("srcLabelArray", array($"srcLabels".cast("string")))
      .withColumn("dstLabelArray", array($"dstLabels".cast("string")))

    // Add binary columns for properties
    val binDF = allProperties.foldLeft(withArrayDF) { (tempDF, prop) =>
      tempDF.withColumn(
        s"prop_$prop",
        when(array_contains($"propertiesArray", prop), 1.0).otherwise(0.0)
      )
    }

    // Apply Word2Vec to relationshipType, srcLabels, and dstLabels
    val word2VecRel = new Word2Vec()
      .setInputCol("relationshipTypeArray")
      .setOutputCol("relVector")
      .setVectorSize(3)
      .setMinCount(0)

    val relModel = word2VecRel.fit(binDF)
    val withRelVec = relModel.transform(binDF)

    val word2VecSrc = new Word2Vec()
      .setInputCol("srcLabelArray")
      .setOutputCol("srcVector")
      .setVectorSize(3)
      .setMinCount(0)

    val srcModel = word2VecSrc.fit(withRelVec)
    val withSrcVec = srcModel.transform(withRelVec)

    val word2VecDst = new Word2Vec()
      .setInputCol("dstLabelArray")
      .setOutputCol("dstVector")
      .setVectorSize(3)
      .setMinCount(0)

    val dstModel = word2VecDst.fit(withSrcVec)
    val withDstVec = dstModel.transform(withSrcVec)

    // Assemble features
    val propCols = allProperties.toArray.sorted.map(p => s"prop_$p")
    val assembler = new VectorAssembler()
      .setInputCols(Array("relVector", "srcVector", "dstVector") ++ propCols)
      .setOutputCol("features")

    val finalDF = assembler.transform(withDstVec)
    finalDF
  }
}
