import logging
import os
from azure.eventhub import EventHubClient, Sender, EventData, Offset

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("azure")

#
ADDRESS = os.environ['EH_ADDRESS']
USER = os.environ['EH_USER']
KEY = os.environ['EH_KEY']

total = 0
last_sn = -1
last_offset = "-1"

client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
receiver_part0 = client.add_receiver(consumer_group="$Default", partition="0",prefetch=5000, offset=Offset("-1"))
receiver_part1 = client.add_receiver(consumer_group="$Default", partition="1",prefetch=5000, offset=Offset("-1"))

receivers = [receiver_part0, receiver_part1]

try:
    client.run()

    for receiver in receivers:
    
        batched_events = receiver.receive(timeout=5000)
        total=0
        while batched_events:

            logger.info("Batch event lenght : {}".format(len(batched_events)))

            for event_data in batched_events:
                event_offset = event_data.offset.value
                event_sn = event_data.sequence_number
                event_enqueued_time = event_data.enqueued_time
                total += 1
                logger.info("Partition {}, Received {}, sn={} offset={} enqueueTime={}".format(
                    "0",
                    total,
                    event_sn,
                    event_offset,
                    event_enqueued_time))
            
            batched_events = receiver.receive(timeout=5000,max_batch_size=100)
            total=0
            
except:
    raise
finally:
    client.stop()
