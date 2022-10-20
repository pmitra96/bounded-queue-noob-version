from message import Message
class Publisher():

    def __init__(self,id,bounded_message_queue):
        self.message_queue = bounded_message_queue
        self.id = id
        self.message_queue = bounded_message_queue

    def publish_message(self,message_data,topic = "all"):
        message = Message(topic=topic,message=message_data)
        print(f'publishing message {message.message}')
        self.message_queue.publish(message)