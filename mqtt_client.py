#!/usr/bin/env python3

'''
MQTT client for publishing sensor data
'''

import paho.mqtt.client as mqtt

class MqttClient:
    def __init__(self, broker_address):
        print("MqttClient: Starting connection to {}".format(broker_address))
        self.client = mqtt.Client()
        self.client.connect(broker_address)

    def publish(self, topic, value):
        print("MqttClient: Publishing...")
        self.client.publish(topic, value)

    def loop(self, timeout):
        self.client.loop(timeout=timeout)

    def subscribe(self, topic, handler):
        self.client.on_message = handler
        self.client.subscribe(topic)

if __name__ == "__main__":
    import time

    client = MqttClient("localhost")

    def handler(client, userdata, message):
        print("Received message '" + str(message.payload) + "' on topic '"
                        + message.topic + "' with QoS " + str(message.qos))

    client.subscribe("#", handler)
    client.publish("/foo/bar/baz/temperature", "25.7")

    for i in range(1, 5):
        client.loop()
        time.sleep(1)
