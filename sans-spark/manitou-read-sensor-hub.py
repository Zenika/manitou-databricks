import logging
import os
import os
from azure.eventhub import EventHubClient, Offset

logging.basicConfig(level=logging.INFO)


connection_str = "Endpoint=sb://{}/;SharedAccessKeyName={};SharedAccessKey={};EntityPath={}".format(
    "xxx",
    "xxxx",
    "xxxx",
    "xxx")
client = EventHubClient.from_connection_string(connection_str)

receiver = client.add_receiver(consumer_group="rrrrr", partition="0", offset=Offset('-1'))
try:
    client.run()
    logger = logging.getLogger("azure.eventhub")
    received = receiver.receive(timeout=60, max_batch_size=100)
    for event_data in received:
        logger.info("Message received:{}".format(event_data.application_properties))
except:
    raise
finally:
    client.stop()