import paho.mqtt.client as mqtt
import json
import logging
import os

class MqttPublisher:
    def __init__(self):
        self.broker = os.getenv("MQTT_HOST")
        self.port = int(os.getenv("MQTT_PORT", 1883))
        self.username = os.getenv("MQTT_USER")
        self.password = os.getenv("MQTT_PASSWORD")
        self.topic_prefix = os.getenv("MQTT_TOPIC_PREFIX", "sgcc")
        self.client_id = f"sgcc_electricity_{os.getenv('HOSTNAME', 'local')}"
        
        # 使用最新的回调API版本
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

    def publish(self, user_id, data):
        if not self.broker:
            logging.warning("MQTT_HOST not set, skipping MQTT publish")
            return

        try:
            logging.info(f"Connecting to MQTT broker {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port, 60)
            
            # 发布总数据到主 topic
            main_topic = f"{self.topic_prefix}/{user_id}"
            payload = json.dumps(data, ensure_ascii=False)
            self.client.publish(main_topic, payload, retain=True)
            logging.info(f"Published main data to MQTT topic: {main_topic}")

            # 也可以分项发布，方便某些系统集成
            for key, value in data.items():
                sub_topic = f"{main_topic}/{key}"
                self.client.publish(sub_topic, str(value), retain=True)
            
            self.client.disconnect()
        except Exception as e:
            logging.error(f"Failed to publish to MQTT: {e}")
