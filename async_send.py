import asyncio
from azure.eventhub import EventHubProducerClient, EventHubSharedKeyCredential  # The package name suffixed with ".aio" for async
from azure.eventhub import EventData
from azure.identity import DefaultAzureCredential
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

event_hub_namespace = os.environ.get('EVENT_HUB_NAMESPACE')
event_hub_name = os.environ.get('EVENT_HUB_NAME')
shared_access_policy = os.environ.get('EVENT_HUB_SAS_POLICY')
shared_access_key = os.environ.get('EVENT_HUB_SAS_KEY')
credential = EventHubSharedKeyCredential(shared_access_policy, shared_access_key)


# read parameters files
# config = json.load(open('parameters.json'))

# fully_qualified_namespace = config["EVENT_HUB_NAMESPACE"]
# eventhub_name = config["EVENT_HUB_NAME"]

async def send():
    client = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential
    )
    try:
        event_data_batch = client.create_batch()
        event_data_batch.add(EventData('custID: 1234, custName: John Doe, custAddress: 123 Main St, custCity: Anytown, custState: NY, custZip: 12345'))
        event_data_batch.add(EventData('custID: 1235, custName: Jane Doe, custAddress: 124 Main St, custCity: Anytown, custState: NY, custZip: 12345'))
        client.send_batch(event_data_batch)
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(send())