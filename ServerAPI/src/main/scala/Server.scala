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

  val projectDir = "C:\\Users\\30694\\Desktop\\HybridLSHSchemaDiscovery\\schemadiscovery"
  var runningProcess: Option[Process] = None
  var clusteringCompleted: Boolean = false //  Νέα μεταβλητή για παρακολούθηση κατάστασης

  val receivedMetrics = ListBuffer.empty[String]
  val receivedNodeInfo = ListBuffer.empty[String]
  val receivedEdgeInfo = ListBuffer.empty[String]



  val corsSettings = CorsSettings.defaultSettings.withAllowedOrigins(HttpOriginMatcher.*)


  val route = cors(corsSettings) {
    path("start") {
      post {
        complete {
          if (runningProcess.isEmpty || !runningProcess.exists(_.isAlive())) {
            clusteringCompleted = false //  Reset πριν ξεκινήσει
            val command = Seq("cmd", "/c", "sbt", "run")
            val pb = new java.lang.ProcessBuilder(command: _*)
            pb.directory(new File(projectDir))
            pb.inheritIO()
            val process = pb.start()
            runningProcess = Some(process)
            "sbt run has started!"
          } else {
            "sbt run is already running!"
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
    }

  }
  

  


  val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)
  println(" Server is online at http://localhost:8080/")
  println(" Waiting for /clustering-finished and metrics")
  println(" Press Enter to stop the server...")

  scala.io.StdIn.readLine()

  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())
}
