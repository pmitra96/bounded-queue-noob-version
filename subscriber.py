class Subscriber():

    def __init__(self,id,bounded_message_queue):
        self.message_queue = bounded_message_queue
        self.id = id
        self.message_queue = bounded_message_queue

    def subscribe(self,topic = "all"):
        self.message_queue.regsiter(self,topic)
    
    def unsubscribe(self,topic = "all"):
        self.message_queue.deregister(self,topic)

    def message_received(self,message):
        print(f'subscriber: {self.id} message received from message queue {message.message}')
