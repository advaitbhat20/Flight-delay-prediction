import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pyspark
import boto3
import pandas as pd
import time
   

if __name__ == "__main__":

    if len(sys.argv) != 3:
        raise IOError("\nInvalid usage; enter correct format: \n example.py <hostname> <port>")

    # Initialize a SparkContext with a name
    sc = SparkContext(appName="Stream-Example")

    pred_schema_lst = ["State"]

    def get_prediction(data: pyspark.rdd.RDD):
        
        cols = ["AIRLINE","FLIGHT_NO","TAIL_NO","ORIGIN_AIRPORT","DEST_AIRPORT","DISTANCE"] 
        dff = pd.DataFrame(columns = cols)

        s3_client =boto3.client('s3')
        s3_bucket_name= "cs-516-project-flight-input"
        s3 = boto3.resource('s3',
                    aws_access_key_id= "############",
                    aws_secret_access_key= "#########")
        my_bucket=s3.Bucket(s3_bucket_name)
        has_data = False
        try:
            print("get prediction:", type(data))
            print("printing")
            rows = data.collect()
            for row in rows:
                has_data = True
                print("Row: ",row)
                cells = row.split(",")
                dff.append(dict(zip(dff.columns, cells)), ignore_index=True)

            print("writing data:", has_data)
            print(dff)
            if has_data:
                dff.to_csv("df_new_" + str(round(time.time() * 1000)) + ".csv",
          storage_options={'key': '##########',
                           'secret': '#########/JWGd'})

            print("printed")
        except:
            print("No data")

    # Create a StreamingContext with a batch interval of 1 seconds
    stc = StreamingContext(sc, 1)

    # Use Checkpointing feature
    # Required for updateStateByKey
    stc.checkpoint("checkpoint")

    # Create a DStream to connect to hostname:port (like localhost:9999)
    lines = stc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    # Function  used to update the current counts
    updateFunc = lambda new_vals, current_count: sum(new_vals) + (current_count or 0)

    state = lines.map(lambda x: x)

    print("statetype:", type(state))
    # Print the current state
    state.pprint()

    state.foreachRDD(get_prediction)

    # Start the computation
    stc.start()

    # Wait for the computation to terminate
    stc.awaitTermination()
