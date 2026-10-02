# %%
import socket
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta


# %%
print(datetime.now().strftime("%H:%M"))
# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='reader'

# %% Streaming Query
with open("pg79701.txt", "r", encoding="utf-8") as file:
  while True:
    line = file.readline()

    # End of file
    if not line:
      break

    producer.produce(
      topic=topic,
      value=line
    )

    print(line)
    time.sleep(1)

producer.flush()

producer.close()
