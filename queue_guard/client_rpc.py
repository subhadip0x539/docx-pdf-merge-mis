import pika, json, random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# channel.queue_declare(queue='hello', auto_delete=False)


json_data = {
  "docIds": [
    "65d21c6cddb6ba5935355388",
    "65d21c6cddb6ba5935355388"
  ],
  "processTag": "PDFMERGE",
  "config": {
    "docOrder": [
      "65d21c6cddb6ba5935355388",
      "65d21c6cddb6ba5935355388"
    ],
    "rotation": {
      "65d21c6cddb6ba5935355388": 90,
      "65d21c6cddb6ba5935355388": 0
    }
  }
}

for i in range(1):
    # data = {"number":random.randint(1,100)}
    channel.basic_publish(exchange='',
                        routing_key='hello',
                        body=json.dumps(json_data)
                        )
    

    print(f" [x] Sent {json_data}")

connection.close()