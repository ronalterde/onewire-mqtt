#!/usr/bin/env python3

"""Publish 1-Wire changes to MQTT broker"""

import time
import json
import sys
import time
import copy
import logging

import paho.mqtt.client
import ow


CONFIG_FILE_PATH = "config.json"
POLL_INTERVAL_SECONDS = 1


class PresenceSensor:
    """Special binary input sensor

    These sensors appear on the bus and disappear, depending on the
    state of their input pin.
    """

    def __init__(self, address, present):
        self.address = address
        self.present = present


class OneWireHal:
    def __init__(self, host_and_port):
        ow.init(host_and_port)

    def __del__(self):
        ow.finish()

    def get_sensor(self, address, attribute):
        """Get sensor object.

        There is a special object returned if 'present' is passed
        as an attribute. Those sensors might not always be available
        on the bus.
        """
        if attribute == "present":
            try:
                ow.Sensor(address).useCache(False)
                return PresenceSensor(address, present=True)
            except ow.exUnknownSensor:
                return PresenceSensor(address, present=False)
        else:
            try:
                return ow.Sensor(address)
            except ow.exUnknownSensor:
                return None


class MqttClient:
    """Wrapper around the paho mqtt client."""

    def __init__(self, broker_address):
        logging.info("MqttClient: Starting connection to {}".format(broker_address))
        self.client = paho.mqtt.client.Client()
        self.client.connect(broker_address)

    def publish(self, topic, value):
        self.client.publish(topic, value)
        logging.info(f"MqttClient: Published '{topic}' : '{value}'")

    def loop(self, timeout):
        self.client.loop(timeout=timeout)

    def subscribe(self, topic, handler):
        self.client.on_message = handler
        self.client.subscribe(topic)


class OneWireUpdater:
    """Maintains a list of sensors."""

    def __init__(self, hal, sensors):
        self.hal = hal

        # Create deep copy because we use the 'value' field as a cache.
        self.sensor_descriptors = copy.deepcopy(sensors)

    def get_changed_sensors(self):
        """Return a list of sensors whose values have changed."""
        changed_sensors = []
        for sensor_descriptor in self.sensor_descriptors:
            sensor_object = self.hal.get_sensor(
                sensor_descriptor["address"], sensor_descriptor["attribute"]
            )
            if (not "value" in sensor_descriptor) or self._value_has_changed(
                sensor_descriptor, sensor_object
            ):
                changed_sensors.append(sensor_descriptor)
            sensor_descriptor["value"] = getattr(
                sensor_object, sensor_descriptor["attribute"]
            )
        return changed_sensors

    def _value_has_changed(self, sensor_descriptor, sensor_object):
        return sensor_descriptor["value"] != getattr(
            sensor_object, sensor_descriptor["attribute"]
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with open(CONFIG_FILE_PATH) as file_handle:
        json_data = json.load(file_handle)
        one_wire_server = f"{json_data['owserver_host']}:{json_data['owserver_port']}"
        mqtt_server = json_data["mqtt_host"]
        onewire_sensors = [
            sensor for sensor in json_data["sensors"] if "sensed" in sensor["attribute"]
        ]

    onewire_updater = OneWireUpdater(OneWireHal(one_wire_server), onewire_sensors)
    mqttclient = MqttClient(mqtt_server)

    while True:
        changed_onewire_sensors = onewire_updater.get_changed_sensors()

        if len(changed_onewire_sensors) > 0:
            logging.info("Changed 1-Wire sensors: {}".format(changed_onewire_sensors))

        for sensor in changed_onewire_sensors:
            if sensor["value"] == "1":
                value = "OFF" if "inverted" not in sensor else "ON"
            elif sensor["value"] == "0":
                value = "ON" if "inverted" not in sensor else "OFF"

            mqttclient.publish(sensor["id"], value)

        mqttclient.loop(timeout=POLL_INTERVAL_SECONDS)
