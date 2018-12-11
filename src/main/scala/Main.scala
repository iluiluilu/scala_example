import org.eclipse.paho.client.mqttv3.{MqttClient, MqttConnectOptions, MqttException, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

object Main {
  def main(args: Array[String]): Unit = {
    val brokerUrl = "tcp://localhost:1883"
    val topic = "test"
    val msg = "Hello world test data"

    var client: MqttClient = null

    // Creating new persistence for mqtt client
    val persistence = new MqttDefaultFilePersistence("/tmp")

    val options:MqttConnectOptions = new MqttConnectOptions
    options.setUserName("linhnt")
    options.setPassword("123456".toCharArray)
    options.setCleanSession(true)
    options.setKeepAliveInterval(3600)
      try {
        // mqtt client with specific url and client id
        client = new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)

        client.connect(options)

        val msgTopic = client.getTopic(topic)
        val message = new MqttMessage(msg.getBytes("utf-8"))

        while (true) {
          msgTopic.publish(message)
          println("Publishing Data, Topic : %s, Message : %s".format(msgTopic.getName, message))
          Thread.sleep(100)
        }
      }

      catch {
        case e: MqttException => println("Exception Caught: " + e)
      }

      finally {
        client.disconnect()
      }
  }
}
