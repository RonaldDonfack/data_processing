# %%
import re

output_file = open("cleaned_book.txt", "w", encoding="utf-8")

from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic='reader'
consumer.subscribe([topic])

# %%
# Configuration
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0

while True:
    msg = consumer.poll(1.0)

    # 1. Handle "No Message" (Timeout)
    if msg is None:
        empty_polls += 1
        if empty_polls >= MAX_EMPTY_POLLS:
            print("Closing: No new messages received.")
            break
        continue

    # 2. Handle Errors
    if msg.error():
        error_count += 1
        print(f"Consumer error: {msg.error()}")
        if error_count >= MAX_ERRORS:
            print("Closing: Too many consecutive errors.")
            break
        continue

    # 3. Handle Success
    # Reset counters when we actually get data
    empty_polls = 0
    error_count = 0
    # Get the incoming message
    text = msg.value().decode("utf-8")

    # Convert to lowercase
    text = text.lower()

    # Remove punctuation
    text = re.sub(r"[^\w\s]", "", text)

    # Remove extra spaces
    text = re.sub(r"\s+", " ", text).strip()

    # Write cleaned text to the output file
    output_file.write(text + "\n")

    print(text)
    print(f"Rcvd message: {msg.value().decode('utf-8')}")

# Clean up
consumer.close()

