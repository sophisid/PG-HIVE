import java.io.{File, IOException}
import scala.sys.process._
import scala.util.{Failure, Success, Try}

object X3MLBatchRunner {

  def runX3MLBatch(
      inputFolder: String,
      x3mlMapping: String,
      policyFile: String,
      x3mlEngineJar: String,
      outputFolder: String = "output_trig"
  ): Unit = {
    val inputDir = new File(inputFolder)
    val outDir = new File(outputFolder)
    if (!outDir.exists()) {
      println(s"[INFO] Creating output directory: ${outDir.getAbsolutePath}")
      outDir.mkdirs()
    }

    val xmlFiles = Option(inputDir.listFiles()).getOrElse(Array.empty).filter(_.getName.endsWith(".xml"))
    if (xmlFiles.isEmpty) {
      println(s"[WARN] No XML files found in $inputFolder.")
      return
    }

    xmlFiles.foreach { file =>
      val trigFile = new File(outDir, file.getName.replaceAll("\\.xml$", ".trig"))
      val cmd = Seq(
        "java", "-Xms10g", "-Xmx12g", "-jar", x3mlEngineJar,
        "--input", file.getAbsolutePath,
        "--x3ml", x3mlMapping,
        "--policy", policyFile,
        "--output", trigFile.getAbsolutePath,
        "--format", "application/trig",
        "--reportProgress"
      )

      println(s"[INFO] Running X3ML for ${file.getName}")
      Try(cmd.!!) match {
        case Success(output) =>
          println(s"[SUCCESS] ${file.getName} → ${trigFile.getName}")
        case Failure(e) =>
          println(s"[ERROR] Failed to process ${file.getName}")
          e.printStackTrace()
      }
    }

    println(s"[DONE] All files processed.")
  }
}
