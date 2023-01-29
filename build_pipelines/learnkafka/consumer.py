#!/usr/bin/env python

#This is the consumer file that tests the 

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer

if __name__ == '__main__':
    #Parse the command line args that are supplied with this script
    parser = ArgumentParser()
    parser.add_argument('config_file',type=FileType('r'))
    args = parser.parse_args()

    #Parse the configuration
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    print(config_parser['default'])
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    consumer = Consumer(config)

    #Subcribe to topic

    topic = 'higheverest'
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("Error: %s".format(msg.error()))
            else:
                print(f'Consumed event from topic {msg.topic()}: key = {msg.key()} value = {msg.value().decode("utf-8")}')
    except:
        pass
    finally:
        consumer.close()
