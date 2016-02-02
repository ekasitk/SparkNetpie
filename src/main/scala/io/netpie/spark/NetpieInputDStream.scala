package io.netpie.spark

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import java.util._
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary.Base64

/**
 * Input stream that subscribe messages from a Mqtt Broker.
 * Uses eclipse paho as MqttClient http://www.eclipse.org/paho/
 * @param brokerUrl Url of remote mqtt publisher (endpoint in microgear.cache), e.g. tcp://localhost:1883
 * @param appId application ID
 * @param appKey application key 
 * @param appSecret application secret
 * @param gearName name of netpie client
 * @param token token in microgear.cache
 * @param secret secret in microgear.cache
 * @param storageLevel RDD storage level.
 */

class NetpieInputDStream(
    _ssc: StreamingContext,
    brokerUrl: String, 
    appId: String,
    appKey: String,
    appSecret: String,
    gearName: String,   
    token: String,     
    secret: String,   
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[String](_ssc) {

  override def name: String = s"Netpie stream [$id]"

  private def ALGORITHM: String = "HmacSHA1"

  def getReceiver(): Receiver[String] = {
    val hkey = secret + "&" + appSecret
    val now = new Date()
    val mqttUser = appKey + "%" + now.getTime()/1000
    val mac = Mac.getInstance(ALGORITHM)
    val signingKey = new SecretKeySpec(hkey.getBytes(),ALGORITHM)
    mac.init(signingKey)
    val digest: Array[Byte] = mac.doFinal((token + "%" + mqttUser).getBytes())
    val base64: Array[Byte] = Base64.encodeBase64(digest)
    val mqttPassword = new String(base64)

    new NetpieReceiver(brokerUrl, appId, token, gearName, mqttUser, mqttPassword, storageLevel)
  }
}

class NetpieReceiver(
    brokerUrl: String,
    appId: String,
    token: String,     
    gearName: String,
    mqttUser: String,
    mqttPassword: String,
    storageLevel: StorageLevel
  ) extends Receiver[String](storageLevel) {

  var client: MqttClient = null

  def onStop() {
    if (client != null) {
       client.unsubscribe("/" + appId + "/gearname/" + gearName)
       client.disconnect
       client = null
    } 
  }

  def onStart() {

    // Set up persistence for messages
    val persistence = new MemoryPersistence()

    // Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    client = new MqttClient(brokerUrl, token, persistence)

    // Callback automatically triggers as and when new message arrives on specified topic
    val callback = new MqttCallback() {

      // Handles Mqtt message
      override def messageArrived(topic: String, message: MqttMessage) {
        val rtop = topic.substring(appId.length()+1)
        if (rtop.startsWith("/&")) {
           Console.err.println("Got unexpected topic " + rtop) // should not happen
        } else {
           val msg = new String(message.getPayload(), "utf-8") 
           println("GOT " + topic + ": " + msg) 
           store(msg)
        }
      }

      override def deliveryComplete(token: IMqttDeliveryToken) {
      }

      override def connectionLost(cause: Throwable) {
        println("Connection lost, restart")
        Console.err.println("Connection lost, restart")
        restart("Connection lost ", cause)
      }
    }

    // Set up callback for MqttClient. This needs to happen before
    // connecting or subscribing, otherwise messages may be lost
    client.setCallback(callback)

    // Connect to MqttBroker
    val options = new MqttConnectOptions()
    options.setUserName(mqttUser)
    options.setPassword(mqttPassword.toCharArray)
    client.connect(options)

    // Subscribe to Mqtt topic
    client.subscribe("/" + appId + "/gearname/" + gearName)

  }
}
