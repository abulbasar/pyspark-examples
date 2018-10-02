import io
import avro.schema
import avro.io
from kafka import KafkaConsumer

# To consume messages
consumer = KafkaConsumer("tnx",
                         group_id="my_group",
                         bootstrap_servers=["localhost:9092"])

schema_path = "transactions.avsc"
schema = avro.schema.Parse(open(schema_path).read())
reader = avro.io.DatumReader(schema)

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    record = reader.read(decoder)
    print(record)



