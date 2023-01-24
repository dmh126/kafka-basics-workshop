package example

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import com.typesafe.config.ConfigFactory
import java.util.Properties
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}

object KafkaStreamsApp extends KafkaSetup with App {
  val config = ConfigFactory.load()
  val props = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-streams-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap_servers"))
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  
  val builder = new StreamsBuilder
  val projectsTable: KTable[DeviceId, Project] = builder.table[DeviceId, Project](TopicProjects)
  val metricsStream: KStream[DeviceId, Measurement] = builder.stream[DeviceId, Measurement](TopicMeasurements).filter((_, measurement) => {
    measurement.value >= 30.0
  })


  metricsStream.join(projectsTable){(measurement, project) =>
    Alert(project.owner, measurement.value, measurement.timestamp)
  }.to(TopicAlerts)

  val topology: Topology = builder.build()

  println(topology.describe())

  val application: KafkaStreams = new KafkaStreams(topology, props)
  application.start()

}

trait KafkaSetup {
  final val TopicUsers = "com.testing.Users"
  final val TopicProjects = "com.testing.Projects"
  final val TopicMeasurements = "com.testing.Measurements"

  final val TopicAlerts = "com.testing.Alerts"

  type UserId = String
  type ProjectId = String
  type DeviceId = String
  case class User(id: UserId, name: String)
  case class Project(id: ProjectId, name: String, owner: UserId, device: DeviceId)
  case class Measurement(device: DeviceId, value: Double, timestamp: String)
  case class Alert(user: UserId, value: Double, timestamp: String)

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }
}
