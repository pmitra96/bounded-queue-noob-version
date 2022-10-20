from collections import deque,defaultdict

class DependencyResolver:
    @staticmethod
    def find_execution_order(consumer_ids,consumer_dependency_list):
        if len(consumer_dependency_list) == 0:
            return consumer_ids
        in_degree = defaultdict(lambda: 0)
        preqs = {}
        for dependency_edge in consumer_dependency_list:
            consumer = dependency_edge.consumer
            prerequisite = dependency_edge.consumer_preq
            preqs.setdefault(prerequisite,[]).append(consumer)
        for i in consumer_ids:
            for j in preqs.get(i,[]):
                if j in in_degree:
                    in_degree[j]+=1
                else:
                    in_degree[j] = 1
        q = deque([])
        for i in consumer_ids:
            if in_degree[i] == 0:
                q.append(i)
        num_visited = 0
        res = []
        while q:
            top = q.popleft()
            num_visited+=1
            res.append(top)
            for req in preqs.get(top,[]):
                    in_degree[req]-=1
                    if in_degree[req] == 0:
                        q.append(req)
        if num_visited == len(consumer_ids):
            return res
        else:
            return []
                    
                
                
                
        
        
        
        