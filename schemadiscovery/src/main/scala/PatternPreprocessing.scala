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
}
