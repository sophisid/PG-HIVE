import java.nio.file.{Files, Path, Paths}
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Own only files created by this application; keep large data off the driver. */
object MemoryStages {
  private val owned = ArrayBuffer.empty[Path]
  private var checkpointRoot: Option[Path] = None
  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run(): Unit = {
      try cleanup()
      catch { case error: Exception => System.err.println("Staging cleanup failed: " + error) }
    }
  }, "pg-hive-owned-staging-cleanup"))

  def newDirectory(prefix: String): Path = synchronized {
    val parent = Paths.get(sys.env.getOrElse("PG_HIVE_TEMP_DIR",
      System.getProperty("java.io.tmpdir"))).toAbsolutePath
    Files.createDirectories(parent)
    val directory = Files.createTempDirectory(parent, "pg-hive-" + prefix + "-")
    owned += directory
    directory
  }

  def remove(directory: Path): Unit = synchronized {
    require(owned.contains(directory), "Refusing to remove an unowned staging directory")
    if (Files.exists(directory)) {
      val walk = Files.walk(directory)
      try walk.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
      finally walk.close()
    }
    owned -= directory
  }

  def snapshot(df: DataFrame): DataFrame = {
    synchronized {
      if (checkpointRoot.isEmpty) {
        checkpointRoot = Some(newDirectory("checkpoints"))
        df.sparkSession.sparkContext.setCheckpointDir(checkpointRoot.get.toString)
      }
    }
    // A reliable checkpoint is disk-backed and severs upstream references.
    // Materialize it before releasing any ancestor cache.
    df.checkpoint(eager = true)
  }

  def cleanup(): Unit = synchronized {
    owned.toVector.reverse.foreach(remove)
    checkpointRoot = None
  }
}
