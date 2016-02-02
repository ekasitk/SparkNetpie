#!/usr/bin/env node

const APPKEY    = 'AdG4gW6HoTGTpn6';
const APPSECRET = 'XXXXXXXXXXX';
const APPID     = 'cpuloadtest';
const GEARNAME = 'suriya';
const KAFKATOPIC = 'test';
const ZOOKEEPER_CONNECTSTRING = 'localhost:2181';

//
// Test with kafka
// ./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test
// 

var os = require("os"),
    cpus = os.cpus(),
    kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(ZOOKEEPER_CONNECTSTRING),
    producer = new Producer(client),
    MicroGear = require('microgear');

var microgear = MicroGear.create({
    gearkey : APPKEY,
    gearsecret : APPSECRET
});

microgear.on('connected', function() {
    console.log('Connected...');
    microgear.setname(GEARNAME);
});

microgear.on('message', function(topic,body) {
        payloads = [
        { topic: KAFKATOPIC, messages: body, partition: 0 },
        ];

        producer.send(payloads, function(err, data){
        console.log(data)
        });

    console.log('incoming : '+topic+' : '+body);
});



//producer.on('error', function(err){});

microgear.on('closed', function() {
    console.log('Closed...');
});

microgear.connect(APPID);
