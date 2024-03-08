import json
from hit_request import run
import asyncio
from aio_pika import connect


async def callback(message):
    body = message.body
    request_json = json.loads(str(body.decode('utf-8')))
    print(f" [x] Received {request_json}")
    result = await run(request_json)
    print(f" [x] Result {result}")


async def main():
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue("hello")

        # Start listening the queue with name 'hello'
        await queue.consume(callback)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())