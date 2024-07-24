import time

import cantools
import logging
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point

from cantools.database import DecodeError

DBC_PATH = "Solis-EV4.dbc"
InfluxConfig = {
    "bucket": "bucket",
    "org": "Solis",
    "token": "token",
    "url": "https://eu-central-1-1.aws.cloud2.influxdata.com"
}


class DBCParser(object):
    def __init__(self):
        self.dbc = cantools.db.load_file(DBC_PATH, database_format="dbc", encoding="cp1252")
        self.client = influxdb_client.InfluxDBClient(
            url=InfluxConfig["url"],
            token=InfluxConfig["token"],
            org=InfluxConfig["org"]
        )

    def getWriteApi(self):
        return self.client.write_api(write_options=SYNCHRONOUS)

    def parse_message(self, frame_id: int, data: bytes):

        try:
            message = self.dbc.get_message_by_frame_id(frame_id=frame_id)
            logging.debug(f"Message {message}")
        except DecodeError or ValueError or KeyError:
            logging.warning(f"Could not get message for frame {frame_id}")
            return None

        try:
            fields = message.decode_simple(data=data)
            logging.debug(f"Message fields: {fields}")
        except DecodeError:
            logging.warning(f"Could not get fields from message with data {data}")
            return None

        influx_point = Point.measurement(message.name.split('_')[0]).tag("ecu", message.senders[0]).time(time.time_ns()).field(fields)

        return influx_point
