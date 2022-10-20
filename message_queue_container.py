from threading import Semaphore,Lock

# Thread safe message queue: Throws MessageQueueEmptyException,MessageQueueFullException
class MessageQueueContainer():
    def __init__(self,capacity):
        self.rear = capacity-1 
        self.front = 0  
        self.capacity = capacity
        self.array = [None]*capacity
        self.size = 0
        self.enque_semaphore = Semaphore(self.capacity)
        self.dequeue_semaphore = Semaphore(0) 
        self.lock = Lock()

    # enqueue inserts an item at rear
    def enqueue(self,key):
        self.enque_semaphore.acquire()
        if self.size == self.capacity:
            raise MessageQueueFullException(f'Message queue size {self.size} has reached its maximum capacity {self.capacity}')
        else:
            with self.lock:
                self.rear = (self.rear+1) % self.capacity 
                self.array[self.rear] = key
                self.size = self.size + 1
        self.dequeue_semaphore.release()

    def dequeue(self):
        self.dequeue_semaphore.acquire()
        if self.size == 0:
            self.enque_semaphore.release()
            raise MessageQueueEmptyException(f'trying to dequeue when message queue is empty,size: {self.size}')
        else:
            key = None
            with self.lock:
                key = self.array[self.front]
                self.array[self.front] = None
                self.front = (self.front + 1) % self.capacity
                self.size = self.size -1
            self.enque_semaphore.release()
            return key

    ## to just read the value at queue front 
    def get_front(self):
        return self.array[self.front]

    def is_full(self):
        return self.size == self.capacity

    def is_empty(self):
        return self.size == 0 

    def __str__(self):
        return (" ,".join(list(map(str,self.array))))

    def __repr__(self):
        return (" ,".join(list(map(str,self.array))))


## Exceptions thrown my message queue
class MessageQueueException(Exception):
    def __init__(self,description):
        self.description = description
        super().__init__(self.description)    

class MessageQueueEmptyException(MessageQueueException):
    def __init__(self,description):
        super().__init__(description=description)
    
class MessageQueueFullException(Exception):
    def __init__(self,description):
        super().__init__(description=description)
