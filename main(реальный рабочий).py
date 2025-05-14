from fastapi import FastAPI
from kafka import KafkaConsumer
import struct
import requests
import io
import avro.schema
import avro.io

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

def get_avro_schema(schema_id: int):
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    response = requests.get(url)
    response.raise_for_status()
    return avro.schema.parse(response.json()["schema"])

def create_consumer(topic: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"group-{topic}",
        value_deserializer=lambda m: m  # raw bytes
    )

def parse_avro_messages(consumer: KafkaConsumer, limit=10):
    results = []

    for message in consumer:
        raw = message.value
        try:
            magic, schema_id = struct.unpack(">bI", raw[:5])
            if magic != 0:
                raise ValueError("Invalid Avro magic byte")

            schema = get_avro_schema(schema_id)
            bytes_reader = io.BytesIO(raw[5:])
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            record = reader.read(decoder)

            results.append(record)

        except Exception as e:
            results.append({
                "error": str(e),
                "raw": raw.hex()
            })

        if len(results) >= limit:
            break

    return results


@app.get("/parsed")
def get_parsed_sales():
    consumer = create_consumer("SALES")
    return parse_avro_messages(consumer)


@app.get("/aggregated")
def get_aggregated_sales():
    consumer = create_consumer("SALES_AGGREGATE")
    return parse_avro_messages(consumer)
