from message_queue import MessageQueue
from publisher import Publisher
from subscriber import Subscriber
import json

def main():
    message_queue = MessageQueue.get_instance()
    
    
    subscriber_1 = Subscriber(0,message_queue)
    subscriber_1.subscribe("name")
    subscriber_2 = Subscriber(1,message_queue)
    subscriber_2.subscribe("name")
    message_queue.add_dependency(0,1)
    message_dict = {}
    message_dict["name"] = "Pushya"
    message_dict["email"] = "pmitra96@gmail.com"
    publisher_1 = Publisher(0,message_queue)
    publisher_1.publish_message(message_dict,"name")   


if __name__ == "__main__":
    main()