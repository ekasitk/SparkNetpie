Introduction
------------

SparkNetpie provides a Spark streaming receiver for Netpie.

Dependencies
------------
spark-assembly.jar
commons-codec.jar  
org.eclipse.paho.client.mqttv3.jar

Usage
-----

The following Netpie parameter are required. 
- appId application ID
- appKey application key 
- appSecret application secret
- gearName name of Netpie client
- brokerUrl Url of remote mqtt publisher (endpoint in microgear.cache), e.g. tcp://localhost:1883
- token token in microgear.cache
- secret secret in microgear.cache

The appId, appKey and appSecret can be found in Netpie key management which are the same for each application. The gearName is a name of the Netpie client. The brokerUrl, token, and secret are assigned from Netpie server for each particular gearName. 

Typically, we can use microgear (with a specific gearName) to make a connection to Netpie. The microgear.cache file will be created. Then, stop the microgear. Look into the cache file to get brokerUrl, token and secret information. Notice that the brokerUrl uses tcp:// scheme.

Example
-------
Some example programs can be found in test directory.

TODO
----
Make maven project
