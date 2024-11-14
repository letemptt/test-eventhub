import asyncio
from azure.eventhub import EventData,EventHubSharedKeyCredential
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
import json,os

eventhub_namespace = os.environ['EVENT_HUB_HOSTNAME']
eventhub_name = os.environ['EVENT_HUB_NAME']
shared_access_policy = os.environ['EVENT_HUB_SAS_POLICY']
shared_access_key = os.environ['EVENT_HUB_SAS_KEY']
credential = EventHubSharedKeyCredential(shared_access_policy, shared_access_key)

consumer_client = EventHubConsumerClient(
    fully_qualified_namespace=eventhub_namespace,
    consumer_group='$Default',
    eventhub_name=eventhub_name ,
    credential=credential,
)
with consumer_client:
    pass # consumer_client is now ready to be used.

#producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

def send_event(producer):
    # Without specifying partition_id or partition_key
    # the events will be distributed to available partitions via round-robin.
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData('custID: 1234, custName: John Doe, custAddress: 123 Main St, custCity: Anytown, custState: NY, custZip: 12345'))
    event_data_batch.add(EventData('custID: 1235, custName: Jane Doe, custAddress: 124 Main St, custCity: Anytown, custState: NY, custZip: 12345'))
    producer.send_batch(event_data_batch)
    print("Events sent successfully.")


