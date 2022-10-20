from threading import Lock
from message_queue_container import MessageQueueContainer
import config
from dependency_resolver import DependencyResolver

class DependencyEdge:
    
    def __init__(self,consumer,consumer_preq):
        self.consumer = consumer
        self.consumer_preq = consumer_preq
    
## Singleton class
class MessageQueue:
    __instance = None
    __singeton_lock = Lock()
    
    def get_instance():
        if not MessageQueue.__instance:
            with MessageQueue.__singeton_lock:
                if not MessageQueue.__instance:
                    MessageQueue.__instance = MessageQueue(capacity=config.MESSAGE_QUEUE_SIZE)
        return MessageQueue.__instance

    def __init__(self,capacity):
        self.capacity = capacity
        self.topic_subscribers = {}
        # container is thread safe
        self.id_to_subscribers = {}
        self.dependencies = []
        self.container = MessageQueueContainer(capacity=self.capacity)

    def regsiter(self,subscriber,topic = "all"):
        print(f'registering subscriber {subscriber.id} for topic {topic}')
        self.topic_subscribers.setdefault(topic,set()).add(subscriber.id)
        self.id_to_subscribers[subscriber.id] = subscriber

    def deregister(self,subscriber,topic = "all"):
        print(f'de-registering subscriber {subscriber.id} for topic {topic}')
        self.topic_subscribers.get(topic,set()).remove(subscriber.id)
        if subscriber.id in self.id_to_subscribers:
            del self.id_to_subscribers[subscriber.id]

    def add_dependency(self,consumer_id,consumer_preq_id):
        depdendency_edge = DependencyEdge(consumer=consumer_id,consumer_preq=consumer_preq_id)
        self.dependencies.append(depdendency_edge)

    def publish(self,message):
        self.container.enqueue(message)
        self.boardcast()

    def get_eligible_subscriber_list(self,message):
        topic = message.topic
        # subscriber that subscribed to all topics
        all_topic_subscribers = self.topic_subscribers.get("all",set())
        specific_topic_subscribers = self.topic_subscribers.get(topic,set())
        return list(all_topic_subscribers.union(specific_topic_subscribers))
        
    def boardcast(self):
        message = self.container.dequeue()
        eligible_subscriber = self.get_eligible_subscriber_list(message=message)
        resolved_dependency_order = DependencyResolver.find_execution_order(eligible_subscriber,self.dependencies)
        for subscriber_id in resolved_dependency_order:
            if subscriber_id in self.id_to_subscribers:
                self.id_to_subscribers[subscriber_id].message_received(message)




    
    