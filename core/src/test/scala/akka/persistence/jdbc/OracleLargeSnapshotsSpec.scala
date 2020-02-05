package akka.persistence.jdbc

import akka.actor.ActorSystem
import akka.persistence.PersistentActor
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SaveSnapshotFailure
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.Done
import akka.actor.Props
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

/**
 * 
 * To re-create start the docker image in scipts:
 * 
 * docker-compose up -d oracle
 * 
 * That forwards the ports to localhsot
 */
object Test extends App {

  val config = ConfigFactory.parseString("""

akka.loglevel = DEBUG 

akka.persistence {
  journal.plugin = jdbc-journal
  snapshot-store.plugin = jdbc-snapshot-store
}

jdbc-journal {
 slick = ${slick}
}

jdbc-snapshot-store {
 slick = ${slick}
}

jdbc-read-journal {
  slick = ${slick}
}


slick {
  profile = "slick.jdbc.OracleProfile$"
  db {
      host = localhost 
      host = localhost 
      url = "jdbc:oracle:thin:@//localhost:1521/xe"
      user = "system"
      user = system
      password = "oracle"
      driver = "oracle.jdbc.OracleDriver"
      numThreads = 5
      maxConnections = 5
      minConnections = 1
    }
 }

""").resolve()
  val system = ActorSystem("cats", config)

  /*
   * 200 000 035
   */
  class LargeSnapshots(val persistenceId: String) extends PersistentActor with ActorLogging {
    var lastSender: Option[ActorRef] = None
    def receiveCommand: Receive = {
      case SaveSnapshotSuccess(_) =>
        lastSender.foreach(_ ! Done)
        log.info("Done")
      case SaveSnapshotFailure(meta, t) =>
        log.error(t, "Snapshot failed {}", meta)
      case command =>
        val largeSnapshot = Array.fill(30 * 1000000)(0) // java serialization so expect 4x payloads
        saveSnapshot(largeSnapshot)
        lastSender = Some(sender())
    }
    def receiveRecover: Receive = {
      case msg => println("Recover: " + msg)
    }
  }

  implicit val timeout = Timeout(20.seconds)
  import akka.pattern.ask

  val ref = system.actorOf(Props(new LargeSnapshots("1")))
  //Await.result((ref ? "save"), Duration.Inf)

  //println("Snapshot saved")

}
