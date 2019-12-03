from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json


from datetime import datetime as dt
from datetime import timezone
from time import localtime, strftime
from pytz import timezone
import pytz
import json

paris = timezone('Europe/Paris')
fmt = '%Y-%m-%d %H:%M:%S %Z%z'

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
                    "xxxx",
                    "exxxxx",
                    "ccccc")

# Create the positions
starting_event_position = {
  "offset": "@latest",  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

eh_conf = {
  'eventhubs.consumerGroup' : "tofirebase-streaming",
  'eventhubs.connectionString' : connection_str,
  'eventhubs.startingPosition' : json.dumps(starting_event_position)
#  ,
#  'eventhubs.endingPosition' : json.dumps(endingEventPosition)
}



event_hubs_df = spark.readStream \
  .format("eventhubs") \
  .options(**eh_conf) \
  .load()

df = event_hubs_df.withColumn("body", event_hubs_df["body"].cast("string"))


### Firebase sample
import pyrebase

config = {
  "apiKey": "xxxxxx",
  "authDomain": "xxxxx.firebaseapp.com",
  "databaseURL": "https://xxxxx.firebaseio.com",
  "storageBucket": "xxxxxx.appspot.com"
}

firebase = pyrebase.initialize_app(config)
db = firebase.database()

def saveToFirebase(row):
  db.child("ccccc").push(row)


df.select("body") \
    .writeStream \
    .foreach(saveToFirebase) \
    .start() \
    .awaitTermination()