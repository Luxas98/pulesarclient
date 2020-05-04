import io
import os
import pulsar
import _pulsar
import fastavro
from fhirfastavro import avroutil
from pulsar.schema import Schema

pulsar_address = os.environ.get("PULSAR_ADDRESS")

if not pulsar_address:
    print('Pulsar address not set')
    exit(1)


class AvroSchema(Schema):
    # Custom AvroSchema for pulsar, unfortunatelly pulsar AvroSchema does not
    # support namespaced schemas spread accross multiple files
    # and expects one big schema file

    # This a mix of BytesSchema and original AvroSchema

    def __init__(self, record_cls=None, schema_definition=None,
                 schema_name=None):
        super().__init__(record_cls, _pulsar.SchemaType.BYTES,
                         schema_definition, schema_name)
        self._schema = schema_definition

    def encode(self, obj):
        fastavro.validate(obj, self._schema)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, self._schema, obj)
        return buffer.getvalue()

    def decode(self, data):
        buffer = io.BytesIO(data)
        d = fastavro.schemaless_reader(buffer, self._schema)
        return d

    def _validate_object_type(self, obj):
        return


service_url = f"pulsar://{pulsar_address}:6650"
client = pulsar.Client(service_url)

fastavro_schema = avroutil.get_bundle_schema()
fastavro_schema = AvroSchema(schema_definition=fastavro_schema,
                                 schema_name=fastavro_schema['name'])


def publisher(topic_name, schema):
    return client.create_producer(topic_name, schema=schema)


def publish(publisher, data, meta):
    publisher.send(data, properties=meta)


def subscribe(topic_name, subscription_name, schema, callback,
              consumer_type=_pulsar.ConsumerType.Shared):
    consumer = client.subscribe(topic_name,
                                subscription_name,
                                consumer_type=consumer_type,
                                schema=schema)

    while True:
        msg = consumer.receive()
        try:
            callback(msg)
            consumer.acknowledge(msg)
        except:
            consumer.negative_acknowledge(msg)

    client.close()


def callback_info(message):
    return message.value(), message.properties()


def close():
    client.close()
