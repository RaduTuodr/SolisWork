import logging
import struct

from DBCParser import DBCParser
from pprint import pprint

from influxdb_client import Point


class DBCtoInflux:
    def __init__(self):
        self.parser = DBCParser()

    def read_data(self, load) -> Point | None:

        try:
            timestamp, frame_id, value_h, value_l = self.process_load(load)

            # TODO: modify value_h and value_l into array of bytes
            #                 (data param is in byte-array format)
            value_h = value_h.encode('utf-8')
            data_h = bytes(struct.unpack('BBBB', value_h))

            value_l = value_l.encode('utf-8')
            data_l = bytes(struct.unpack('BBBB', value_l))

            point = self.parser.parse_message(frame_id, data_h + data_l)
            return point

        except ValueError:
            logging.warning("Invalid Data")
            return None

    @staticmethod
    def process_load(load):
        """

        ex: 21982 : 0x404 : 0x01010101 0x06030804

        :param load: line of information from SD card
        :return: parsed data
        """

        info = load.split(' : ')
        timestamp = info[0]
        frame_id = info[1]
        info = info[2].split(' ')
        value_h = info[0]
        value_l = info[1]

        return timestamp, frame_id, value_h, value_l


if __name__ == '__main__':
    dbcParser = DBCParser()
    dbc = dbcParser.dbc

    pprint(dbc.get_message_by_frame_id(0x200).signals[0].name)  # ex
