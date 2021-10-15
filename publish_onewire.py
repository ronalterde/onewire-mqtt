#!/usr/bin/env python3

'''
Publish 1-Wire changes to MQTT broker
'''

import time
import json
import sys
import time
import copy

import paho.mqtt.client
import ow

'''
Polling interface to 1-Wire sensor bus
Based on 'onewire filesystem'
'''

CONFIG_FILE_PATH='config.json'

class PresenceSensor():
    def __init__(self, present, address):
        self.present = present
        self.address = address


class OneWireHal:
    def __init__(self, host_and_port):
        ow.init(host_and_port)

    def __del__(self):
        ow.finish()

    def get_sensor(self, address, attribute):
        if attribute == 'present':
            # For 'present' sensor, return a special object.
            try:
                ow.Sensor(address).useCache(False)
                return PresenceSensor(True, address)
            except ow.exUnknownSensor:
                return PresenceSensor(False, address)
        else:
            try:
                return ow.Sensor(address)
            except ow.exUnknownSensor:
                return None


class MqttClient:
    def __init__(self, broker_address):
        print("MqttClient: Starting connection to {}".format(broker_address))
        self.client = paho.mqtt.client.Client()
        self.client.connect(broker_address)

    def publish(self, topic, value):
        print("MqttClient: Publishing...")
        self.client.publish(topic, value)

    def loop(self, timeout):
        self.client.loop(timeout=timeout)

    def subscribe(self, topic, handler):
        self.client.on_message = handler
        self.client.subscribe(topic)


'''
Update a list of sensors and return a list of changed ones
'''
class OneWireUpdater:
    def __init__(self, hal, sensors):
        self.hal = hal
        self.sensor_descriptors = copy.deepcopy(sensors) # Deep copy b/c we use the 'value' field as cache.

    '''
    Return a list of sensors whose values have changed.
    '''
    def get_changed_sensors(self):
        changed_sensors = []
        for sensor_descriptor in self.sensor_descriptors:
            sensor_object = self.hal.get_sensor(sensor_descriptor['address'],
                    sensor_descriptor['attribute'])
            if (not 'value' in sensor_descriptor) or self._value_has_changed(sensor_descriptor, sensor_object):
                changed_sensors.append(sensor_descriptor)
            sensor_descriptor['value'] = getattr(sensor_object, sensor_descriptor['attribute'])
        return changed_sensors

    def _value_has_changed(self, sensor_descriptor, sensor_object):
        return (sensor_descriptor['value'] != getattr(sensor_object, sensor_descriptor['attribute']))


if __name__ == "__main__":
    with open(CONFIG_FILE_PATH) as f:
        j = json.load(f)
        one_wire_server = f"{j['owserver_host']}:{j['owserver_port']}"
        mqtt_server = j['mqtt_host']
        onewire_sensors = [i for i in j['sensors'] if 'sensed' in i['attribute']]

    onewire_updater = OneWireUpdater(OneWireHal(one_wire_server), onewire_sensors)
    mqttclient = MqttClient(mqtt_server)

    # Poll 1-Wire every second
    while True:
        changed_onewire_sensors = onewire_updater.get_changed_sensors()

        if len(changed_onewire_sensors) > 0:
            print("Changed 1-Wire sensors: {}".format(changed_onewire_sensors))

        for sensor in changed_onewire_sensors:
            if sensor['value'] == '1':
                value = 'OFF' if 'inverted' not in sensor else 'ON'
            elif sensor['value'] == '0':
                value = 'ON' if 'inverted' not in sensor else 'OFF'

            mqttclient.publish(sensor['id'], value)

        mqttclient.loop(timeout=1)
