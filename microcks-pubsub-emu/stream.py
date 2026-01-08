from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

PROJECT_ID = "my-project"
TOPIC_ID = "HelloMicrocksAPI-1.0.0-receivedHellos"
SUBSCRIPTION_ID = "stream-out-py"

def get_or_create_subscription(project_id, topic_id, subscription_id):
    """
    Creates a subscription if it doesn't exist, then returns the path.
    """

    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    with subscriber:
        try:
            subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path},
            )
            print(f"Created new subscription: {subscription_path}")
        except AlreadyExists:
            print(f"Subscription already exists: {subscription_path}")
        except NotFound:
            print(f"Error: The Topic '{topic_id}' does not exist. Please create the topic first.")
            raise

    return subscription_path

def receive_messages(sub_path):
    """Receives messages from the subscription."""

    def callback(message):
        print(f"Received: {message.data.decode('utf-8')}")
        message.ack()

    subscriber = pubsub_v1.SubscriberClient()
    stream = subscriber.subscribe(sub_path, callback=callback)
    print(f"Listening for messages...\n")

    with subscriber:
        stream.result(timeout=10)

if __name__ == "__main__":
    sub_path = get_or_create_subscription(PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID)
    receive_messages(sub_path)