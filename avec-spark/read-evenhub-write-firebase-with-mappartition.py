from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json


from datetime import datetime as dt
from datetime import timezone
from time import localtime, strftime
from pytz import timezone
import pytz
import json
import logging

logger = logging.getLogger("tofirebase-batch")

paris = timezone('Europe/Paris')
fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

now_dt = dt.utcnow()
# Change Timezone to UTC in current date
now_dt = now_dt.replace(tzinfo=pytz.utc)

# Convert date to Paris (UTC+1 ou UTC+2 en Summer Time)
loc_dt = now_dt.astimezone(paris) 
#loc_dt = paris.localize(now_dt)

spark = SparkSession \
    .builder \
    .appName("Spark Event Hub to Firebase") \
    .getOrCreate()


connection_str = "Endpoint=sb://{}/;SharedAccessKeyName={};SharedAccessKey={};EntityPath={}".format(
                    "xxxxx",
                    "xxxxx",
                    "xxxxx",
                    "xxxx")

# Create the positions
starting_event_position = {
  "offset": "-1",  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

endingEventPosition = {
  "offset": None,                                             # not in use
  "seqNo": -1,                                                # not in use
  "enqueuedTime": loc_dt.strftime(fmt), # point in time
  "isInclusive": True
}

eh_conf = {
  'eventhubs.consumerGroup' : "tofirebase-batch",
  'eventhubs.connectionString' : connection_str,
  'eventhubs.startingPosition' : json.dumps(starting_event_position),
  'eventhubs.endingPosition' : json.dumps(endingEventPosition)
}



event_hubs_df = spark.read \
  .format("eventhubs") \
  .options(**eh_conf) \
  .load()

df = event_hubs_df.withColumn("body", event_hubs_df["body"].cast("string"))

### Firebase sample


def init_firebase_database_connection(partition):
  import pyrebase

  config = {
    "apiKey": "xxxxx",
    "authDomain": "xxxxx.firebaseapp.com",
    "databaseURL": "https://xxxxx.firebaseio.com",
    "storageBucket": "xxxxxx.appspot.com"
  }

  firebase = pyrebase.initialize_app(config)
  db = firebase.database()

  for row in partition:
    final_row = (row,db)
    yield final_row

def saveToFirebase(row):
  logger.info(" Add new Row : ".format(row[0]))
  row[1].child("ccccc").push(row[0])

df.select("body") \
    .rdd \
    .mapPartitions(init_firebase_database_connection) \
    .foreach(saveToFirebase)

spark.sparkContext.stop()