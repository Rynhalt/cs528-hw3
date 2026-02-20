import os
import json
from google.cloud import pubsub_v1
from google.oauth2.credentials import Credentials

PROJECT_ID = "cs528-marcus"
SUBSCRIPTION_ID = "hw3-forbidden-sub"

def main():
    token = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "")
    if not token:
        raise RuntimeError("GOOGLE_OAUTH_ACCESS_TOKEN is not set")

    creds = Credentials(token=token)

    subscriber = pubsub_v1.SubscriberClient(credentials=creds)
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        data = message.data.decode("utf-8", errors="replace")
        print("FORBIDDEN EVENT:", data)
        try:
            print("PARSED:", json.loads(data))
        except Exception:
            pass
        message.ack()

    print(f"Listening on {sub_path} ... Ctrl+C to stop")
    future = subscriber.subscribe(sub_path, callback=callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
        print("Stopped.")

if __name__ == "__main__":
    main()