# import all dependencies
import sys
import json
import collections
import heapq
import math
## The following two are for optional feature
import time
import datetime
# read initial batch of data
initial_file = open(sys.argv[1])
initial_data = []
for line in initial_file:
    initial_data.append(json.loads(line))
initial_file.close()
# Read stream log of data
# Here it is assumed that the .json file does not have any anomalous records
# such as missing data, empty lines. The events are read into a list and each 
# item is a dictionary.
new_data=[]
new_data_file = open(sys.argv[2])
for line in new_data_file:
    new_data.append(json.loads(line))
new_data_file.close()

# times_convert function used for optional feature
def time_convert(time_str):
    """
    time_str(str): %Y-%m-%d %H:%M:%S in string format
    return(float): time in seconds
    """
    return time.mktime(datetime.datetime.strptime(time_str,"%Y-%m-%d %H:%M:%S").timetuple())


# build the customers network class
class network(object):
    def __init__(self, data_set=[]):
        """
        :data_set(List[dict]): initial batch of data of the customer network
        :return: void
        """
        self.time_label=0 # time_label of purchasing events
        self.connections=collections.defaultdict(set) # friend_ids of a customer_id
        self.purchases=collections.defaultdict(collections.deque) # purchase history of customer_id
        self.flagged_purchases=[] # anomalous purchases
        
        self.D=int(data_set[0]['D']) # the number of degrees that defines a user's social network.
        self.T=int(data_set[0]['T']) # the number of consecutive purchases made by a user's social network (not including the user's own purchases)
        
        for data in data_set[1:]:
            self.read(data)
            
    # function read: read data without anomaly detection           
    def read(self, line):
        """
        :line(dict): record of event
        :return: void
        """
        if line['event_type']=='befriend':
            self.befriend(line['id1'], line['id2'])
        elif line['event_type']=='unfriend':
            self.unfriend(line['id1'], line['id2'])
        elif line['event_type']=='purchase':
            self.purchase(line['id'], line['amount'], line['timestamp'])
    
    # function befriend
    def befriend(self, id1, id2):
        """
        :id1(str): first customer_id
        :id2(str): second customer_id
        :return: void
        """
        self.connections[id1].add(id2)
        self.connections[id2].add(id1)
    
    # function unfriend
    def unfriend(self, id1, id2):
        """
        :id1(str): first customer_id
        :id2(str): second customer_id
        :return: void
        """
        self.connections[id1].remove(id2)
        self.connections[id2].remove(id1)
    
    # function purchase: record purchase and the time order
    def purchase(self, customer_id, amount, timestamp):
        """
        :customer_id(str): purchaser id
        :amount(str): amount spent in the purchase
        :timestamp(str): timestamp string of the purchase
        :return: void
        """
        self.purchases[customer_id].append((-self.time_label, amount, timestamp))
        if len(self.purchases[customer_id])>self.T:
            self.purchases[customer_id].popleft()
        self.time_label+=1
    
    # function getstatistic: get the mean and standard deviation of the 
    # last T purchases in the user_id's Dth degree social network.
    def getstatistic(self, customer_id):
        """
        :customer_id(str): purchaser id
        :return: mean(str), sd(str)
        """
        connections=self.getDfriends(customer_id) # first get all users in the user's Dth degree network
        purchases=[] # we will use a heap data structure to keep the T latest purchases.
        for person in connections:
            for purchase in self.purchases[person]: # get all purchases according to time_label
                if len(purchases)<self.T: # we only need to keep up to T latest purchases in the heap.
                    heapq.heappush(purchases,(-purchase[0],purchase[1]))
                else:
                    heapq.heappushpop(purchases,(-purchase[0],purchase[1]))
                    
        amounts=[float(e[1]) for e in purchases]
        # calculate mean and standard deviation
        n=len(amounts)
        mean=sum(amounts)/float(n) if n>0 else 0.
        sd=math.sqrt(sum([(amount-mean)**2 for amount in amounts])/float(n)) if n>0 else 0.
        
        return round(mean,2), round(sd,2)
    
    # function getDfriends: get customer_id's Dth degree network(bfs search)
    def getDfriends(self, customer_id):
        """
        :customer_id(str): purchaser id
        :return connections(set(str)): customer_id's Dth degree network
        """
        visited=set()
        connections=set()
        queue=collections.deque()
        queue.append((customer_id, 0))
        visited.add(customer_id)
        while queue:
            (connection_id, depth)=queue.popleft()
            if depth<=self.D:
                connections.add(connection_id)
            else:
                break
            for e in self.connections[connection_id]:
                if e not in visited:
                    visited.add(e)
                    queue.append((e, depth+1))
        
        connections.remove(customer_id)
        
        return connections
        
    # function read_with_anomaly: read each record with anomaly detection    
    def read_with_anomaly(self, line):
        """
        :line(dict): record of event
        :return: void
        """
        if line['event_type']=='befriend':
            self.befriend(line['id1'], line['id2'])
        elif line['event_type']=='unfriend':
            self.unfriend(line['id1'], line['id2'])
        elif line['event_type']=='purchase':
            customer_id=line['id']
            timestamp=line['timestamp']
            amount=float(line['amount'])
            mean, sd=self.getstatistic(customer_id)
            
            if amount>mean+3.*sd:
                new_line={u'event_type': u'purchase', u'timestamp': timestamp, \
                          u'id': customer_id, u'amount': line['amount'], \
                          u'mean': "%.2f" %mean, u'sd': "%.2f" %sd}
                self.flagged_purchases.append(new_line)
            
            self.purchase(line['id'], line['amount'], line['timestamp'])
            
    # function read_stream: read stream of logs        
    def read_stream(self, data_set=[]):
        """
        :data_set(List[dict]): stream batch of data of the customer network
        :return: void
        """
        for line in data_set:
            self.read_with_anomaly(line)
            
    # function get_rank: This is an optional feature to obtain the most spender
    # in a certain period of time (in units of days). In my implementation, this
    # feature is meaningful only if T is large enough to cover a single customer's 
    # latest purchases in the duration.
    def get_rank(self, curr_time, duration):
        """
        :curr_time(str): current timestamp
        :duration(int): duration in units of days
        :return: List[customer_id(str)]
        """
        # create a dictionary to record the total spending of each customer in the period
        amount_record = collections.defaultdict(float)
        # end_time in seconds
        end_time = time_convert(curr_time)
        # duration in seconds
        period = duration * 24 * 60 * 60
        
        for customer_id in self.purchases.keys():
            for purchase in self.purchases[customer_id]:
                if abs(end_time-time_convert(purchase[2]))<=period:
                    amount_record[customer_id]+=float(purchase[1])
        # sort according to total spending
        ranking = sorted([(amount, customer_id) for customer_id, amount in zip(amount_record.keys(),amount_record.values())],reverse = True)
        print ranking
        return [e[1] for e in ranking]
            
            
# implement network            
social=network(initial_data)
social.read_stream(new_data)
# test optional feature
social.get_rank("2017-06-13 11:33:02",1)
# output anomalies
data_file = open(sys.argv[3],'w')
for line in social.flagged_purchases:
    target = json.dumps(line)
    data_file.write(target+'\n')
data_file.close()