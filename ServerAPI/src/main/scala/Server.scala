import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.collection.mutable.ListBuffer
import java.io.File
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.model.HttpOriginMatcher
// JSON Support
import spray.json._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._


object Server extends App {
  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  //val projectDir = "C:\\Users\\30694\\Desktop\\HybridLSHSchemaDiscovery\\schemadiscovery"   <-----σταθερο Directory
  val projectDir = new File("../schemadiscovery").getCanonicalPath       //<-----μεταβλητο Directory
  var runningProcess: Option[Process] = None
  var clusteringCompleted: Boolean = false //  Νέα μεταβλητή για παρακολούθηση κατάστασης

  val receivedMetrics = ListBuffer.empty[String]
  val receivedNodeInfo = ListBuffer.empty[String]
  val receivedEdgeInfo = ListBuffer.empty[String]



  val corsSettings = CorsSettings.defaultSettings.withAllowedOrigins(HttpOriginMatcher.*)


  val route = cors(corsSettings) {
    path("start") {
      post {
        entity(as[String]) { body =>
          complete {
            if (runningProcess.isEmpty || !runningProcess.exists(_.isAlive())) {
              clusteringCompleted = false

              val args = body.trim.split("\\s+").toList

              if (args.isEmpty) {
                " Missing dataset name. Usage: <dataset> [INCREMENTAL <batchSize>]"
              } else {
                val dataset = args.head
                val isIncremental = args.lift(1).exists(_.equalsIgnoreCase("INCREMENTAL"))
                val batchSize = args.lift(2).getOrElse("")

                val joinedArgs = if (isIncremental) {
                  s"""run $dataset INCREMENTAL $batchSize"""
                } else {
                  s"""run $dataset"""
                }

                val command = Seq("cmd", "/c", "sbt", joinedArgs)

                println(s" Starting: ${command.mkString(" ")}")

                val pb = new java.lang.ProcessBuilder(command: _*)
                pb.directory(new File(projectDir))
                pb.inheritIO()
                val process = pb.start()
                runningProcess = Some(process)

                s" sbt run started with: $joinedArgs"
              }
            } else {
              " sbt run is already running!"
            }
          }
        }
      }
    } ~
    path("stop") {
      post {
        complete {
          runningProcess match {
            case Some(proc) if proc.isAlive =>
              proc.destroy()
              runningProcess = None
              "sbt run has been stopped."
            case _ =>
              "No active sbt run process."
          }
        }
      }
    } ~
    path("metrics-receive") {
      post {
        entity(as[String]) { body =>
          println(s" Received metrics from clustering program:\n$body")
          receivedMetrics += body
          //clusteringCompleted = true //  Εδώ σηματοδοτείται η ολοκλήρωση του clustering
          complete(StatusCodes.OK, "Metrics received successfully.")
        }
      }
    } ~
    path("metrics") {
      get {
        complete(HttpEntity(ContentTypes.`application/json`, receivedMetrics.toList.toJson.compactPrint))
      }
    } ~
    path("clustering-finished") {
      post {
        if (clusteringCompleted) {
          //println(" Responding to /clustering-finished: clustering is completed.")
          complete(StatusCodes.OK, "Clustering completed.")
        } else {
          //println(" Responding to /clustering-finished: clustering still running.")
          complete(StatusCodes.Accepted, "Clustering not completed yet.")
        }
      }
    } ~
    path("set-clustering-complete") {
      post {
        clusteringCompleted = true
        complete(StatusCodes.OK, "Clustering marked as complete.")
      }
    } ~
    path("node-info") {
      post {
        entity(as[String]) { body =>
          println(s"📦 Received node info:\n$body")
          receivedNodeInfo.clear()
          receivedNodeInfo += body
          complete(StatusCodes.OK, "Node info received successfully.")
        }
      }
    } ~
    path("node-info") {
      get {
        complete(HttpEntity(ContentTypes.`application/json`, receivedNodeInfo.toList.toJson.compactPrint))
      }
    } ~
    path("edge-info") {
      post {
        entity(as[String]) { body =>
          println(s"📦 Received edge info:\n$body")
          receivedEdgeInfo.clear()
          receivedEdgeInfo += body
          complete(StatusCodes.OK, "Edge info received successfully.")
        }
      }
    } ~
    path("edge-info") {
      get {
        complete(HttpEntity(ContentTypes.`application/json`, receivedEdgeInfo.toList.toJson.compactPrint))
      }
    } ~
    path("download" / Remaining) { filename =>
      get {
        val fullPath = s"../schemadiscovery/$filename"
        println(s"[DOWNLOAD] Trying to fetch file from: ${new File(fullPath).getCanonicalPath}")
        val file = new File(fullPath)
        if (file.exists() && file.isFile) {
          val source = akka.stream.scaladsl.FileIO.fromPath(file.toPath)
          val contentType = ContentType(MediaTypes.`application/octet-stream`)
          complete(
            HttpResponse(entity = HttpEntity(contentType, source))
              .withHeaders(`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> file.getName)))
          )
        } else {
          complete(StatusCodes.NotFound, s"File not found: $filename")
        }
      }
    }

  }
  

  


  val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)
  println(s"Using projectDir: $projectDir")
  println(" Server is online at http://localhost:8080/")
  println(" Waiting for /clustering-finished and metrics")
  println(" Press Enter to stop the server...")

  scala.io.StdIn.readLine()

  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())
}
