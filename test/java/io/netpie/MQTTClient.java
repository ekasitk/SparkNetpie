package io.netpie;

import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/*
 * microgear.cache
 *
{"_":{"key":"60Q9dXFOySqDTOe","requesttoken":null,"accesstoken":{"token":"VfD3hmI
5xx9v9ZdU","secret":"WHDj3ooLZ62kugEGqnyn5gtiR1oSDeQp","endpoint":"pie://gb.netpi
e.io:1883","revokecode":"AAvWovw8bdFHhZdnibXQFwngrvQ="}}}
 *
 */


public class MQTTClient {
   public static final String ALGORITHM = "HmacSHA1";

   public static void main(String[] args) throws Exception {
      if (args.length < 7) {
         System.err.println("Usage MQTTClient <brokerUrl> <appId> <appKey> <appSecret> <gearName> <token> <secret>");
         System.exit(1);
      }
      String BROKERURL = args[0];
      String APPID = args[1];
      String APPKEY = args[2];
      String APPSECRET = args[3];
      String GEARNAME = args[4];
      String TOKEN = args[5];
      String SECRET = args[6];
      
      String hkey = SECRET + "&" + APPSECRET;
      Date now = new Date();
      String mqttuser = APPKEY + "%" + now.getTime()/1000;

      Mac mac = Mac.getInstance(ALGORITHM);
      SecretKeySpec signingKey = new SecretKeySpec(hkey.getBytes(),ALGORITHM);
      mac.init(signingKey);
      byte[] digest = mac.doFinal((TOKEN + "%" + mqttuser).getBytes());
      byte[] base64 = Base64.encodeBase64(digest);
      String mqttpassword = new String(base64);
      System.out.println("mqttuser=" + mqttuser);
      System.out.println("mqttpassword=" + mqttpassword);
     
      MqttClient client = new MqttClient(BROKERURL,TOKEN,new MemoryPersistence());

      MqttCallback callback = new MqttCallback() {
         /**
          * 
          * connectionLost
          * This callback is invoked upon losing the MQTT connection.
          * 
          */
         @Override
         public void connectionLost(Throwable t) {
            System.out.println("Connection lost!");
            // code to reconnect to the broker would go here if desired
         }

         /**
          * 
          * deliveryComplete
          * This callback is invoked when a message published by this client
          * is successfully received by the broker.
          * 
          */
         @Override
         public void deliveryComplete(IMqttDeliveryToken token) {
            //System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
         }

         /**
          * 
          * messageArrived
          * This callback is invoked when a message is received on a subscribed topic.
          * 
          */
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            System.out.println("-------------------------------------------------");
            System.out.println("| Topic:" + topic);
            System.out.println("| Message: " + new String(message.getPayload()));
            System.out.println("-------------------------------------------------");

         }
      };  

      client.setCallback(callback);
      MqttConnectOptions options = new MqttConnectOptions();
      options.setUserName(mqttuser);
      options.setPassword(mqttpassword.toCharArray());
      client.connect(options);
    
      client.subscribe("/" + APPID + "/gearname/" + GEARNAME);

      Thread.sleep(100000); // wait to get for some messages 

   }
}
