from distutils.command.config import config
from uuid import uuid4
import json
from random import randint
import threading
import logging
import requests
from time import perf_counter
import json
import os
MAX_TIME = 50
MIN_TIME = 20
FOLLOWER = 1
CANDIDATE = 2
LEADER = 4

logging.basicConfig(level=logging.DEBUG)

class Node:
    def __init__(self, server_list, s_id, read_logs, log_file, conf_file):
        # Persistent states, remain same across all terms
        self.id = s_id
        self.current_term = 0
        self.election_timeout = randint(MIN_TIME, MAX_TIME)
        logging.debug(self.election_timeout)
        self.heartbeat_timeout = 10
        
        self.file_lock = threading.Lock()
        self.log_file = log_file

        self.conf_lock = threading.Lock()
        self.conf_file = conf_file
        
        self.voted_for = (None, None)
        self.logs_lock = threading.Lock()
        self.logs = []
        with self.logs_lock:
            for x in read_logs:
                self.logs.append(x)
        self.node_list = []
        self.current_leader = None
        for node in server_list:
            self.node_list.append(node)

        # volatile
        self.state = FOLLOWER
        self.commit_index = len(self.logs)
        self.last_applied = 0
        self.election_timer_thread= None
        self.heartbeat_timer_thread = None
        self.election_lock = threading.Lock()
        self.heartbeat_lock = threading.Lock()
        self.election_start = 0
        self.heartbeat_start = 0

        # for leader, to be reinitialized
        self.next_lock = threading.Lock()
        self.next_index = [len(self.logs) for _ in range(len(self.node_list))]
        self.match_index = [0 for _ in range(len(self.node_list))]


    # RPC to send to follower, run this on new thread and wait for response.
    # If no response send again till u get a response.
    def AppendEntriesReq(self, follower, retAcks):
        logging.debug("Starting Thread for Append RPC from leader to follower " + str(follower[0]))
        message = {
            "term": self.current_term,
            "leader_id": str(self.id),
            "prevLogIndex": self.next_index[follower[0]]-1,
            "prevLogTerm": self.logs[self.next_index[follower[0]]-1] if self.next_index[follower[0]]-1 >= 0 else "NULL",
            "entries": self.logs[self.next_index[follower[0]]:],
            "leaders_commit": self.commit_index
        }
        
        
        logging.debug("Sending Request to " + str(follower[1]) + " " + str(follower[0]))
        res = {}
        tries = 0
        while 'success' not in res.keys() and tries < 2:
            logging.debug("Trying Append")
            try:
                res = requests.post(f"http://localhost:{follower[1]}/appendEntries", json=message)
                res.raise_for_status()
                res = res.json()
                logging.debug("Response "+ str(res))
            except Exception as e:
                logging.debug("Error contacting " + str(follower[1]))
                logging.debug(str(e))
                tries +=1
                continue
        
        if "success" in res.keys() and not res["success"]:
            if res["term"] > self.current_term:
                self.current_term = res['term']
                self._transitionToFollower()
            else:
                with self.next_lock:
                    if self.next_index[follower[0]] > 0:
                        self.next_index[follower[0]] -=  1
                        self.AppendEntriesReq(follower, retAcks)
        elif "success" in res.keys() and res["success"]:
            retAcks[follower[0]] = 1 
            with self.next_lock:
                with self.logs_lock:
                    self.next_index[follower[0]] = len(self.logs)
            logging.debug("Stopping Thread for Append RPC from leader to follower " + str(follower[0]))
            return res
        else:
            return {
                "success": False
            }

    def AppendEntriesRes(self, message):
        logging.debug(str(message))
        self.current_leader = message["leader_id"]
        if message['term'] < self.current_term :
            return {
                "term": self.current_term,
                "success": False
            }
        else:
            self.current_term = message["term"]
        

        if message["prevLogIndex"] == -1 and message["prevLogTerm"] == "NULL":
            with self.logs_lock:
                for i, ele in enumerate(message['entries']):
                    if i < len(self.logs):
                        self.logs[i] = ele
                    else:
                        self.logs.append(ele)
                logging.debug("Logs" + str(self.logs))
            return {
                    "term": self.current_term,
                    "success": True
                }
        elif message['prevLogIndex'] > len(self.logs) or message['prevLogTerm'] != self.logs[message["prevLogIndex"]]:
            return {
                "term": self.current_term,
                "success": False
            }
        else:
            with self.logs_lock:
                for i, ele in enumerate(message['entries']):
                    if message['prevLogIndex'] + 1 + i < len(self.logs):
                        self.logs[message['prevLogIndex'] + i] = ele
                    else:
                        self.logs.append(ele)
                logging.debug("Logs" + str(self.logs))
                if self.commit_index < message['leaders_commit']:
                    with self.file_lock:
                        f = open(self.log_file, "a+")
                        for x in self.logs[self.commit_index:message['leaders_commit']]:
                            f.write(x)
                            f.write("\n")
                        f.close()

                    self.WriteToConfig(self.commit_index, message['leaders_commit'])
                    self.commit_index = message['leaders_commit']
                        
            return {
                    "term": self.current_term,
                    "success": True
                }
        
                  

    def AppendLogEntries(self, message):
        if self.state != 4:
            return {
                "current_leader": self.current_leader,
                "leader_port": self.node_list[int(self.current_leader)][1],
                "success": False
            }
        else:
            with self.heartbeat_lock:
                self.heartbeat_start = perf_counter()
            with self.logs_lock:
                self.logs.append(json.dumps(message))
            retAcks = [0 for _ in range(len(self.node_list))]
            retAcks[self.id] = 1
            logging.debug(self.next_index)
            for node in self.node_list:
                if(node[0] == self.id):
                        continue
                _ = threading.Thread(target=self.AppendEntriesReq, args=(node, retAcks))
                _.start()
            while sum(retAcks) <= len(self.node_list)/2:
                # logging.info("Sum " + str(sum(retAcks)))
                continue
            with self.file_lock:
                f = open(self.log_file, "a+")
                with self.logs_lock:
                    f.write(self.logs[-1])
                    f.write("\n")
                f.close()

            self.WriteToConfig(self.commit_index, self.commit_index+1)
            self.commit_index += 1
            return{
                "success": True
            }
        
    def StartElection(self):
        logging.debug("Starting Election")
        self._transitionToCandidate()
        self.voted_for = (self.current_term, self.id)
        message = {
            "term": self.current_term,
            "candidate_id": str(self.id),
            "lastLogIndex": len(self.logs),
            "lastLogTerm": self.logs[len(self.logs)-1] if len(self.logs) > 0 else "NULL",
        }

        votes = [0 for _ in self.node_list] 
        votes[self.id] = 1
        for node in self.node_list:
            if node[0] == self.id:
                continue
            _ = threading.Thread(target=self._sendVoteReq, args=(node, message, votes))
            _.start()
        
        start = perf_counter()
        while sum(votes) <= len(self.node_list)/2 and (perf_counter() - start) <= 20:
            continue
        
        if sum(votes) > len(self.node_list)/2:
            logging.debug("Transitioning To Leader")
            self._transitionToLeader() # Assume majority and move to leader

    def StartElectionTimer(self):
        logging.debug("Starting new thread for Timer")
        x = threading.Thread(target=self.IncrementElectionTimer)
        self.election_timer_thread = x
        x.start()

    def IncrementElectionTimer(self):
        logging.debug("Timer Started")
        self.election_start = perf_counter()
        nex = self.election_start
        while (nex-self.election_start) <= self.election_timeout and self.state != 4:
            nex = perf_counter()
            if (nex-self.election_start) >= self.election_timeout:
                self.StartElection()
            
        
        logging.debug(f"{nex} {self.election_start}")    
        logging.debug("Exiting Timer")
    
    def StartHeartbeatTimer(self):
        logging.debug("Starting new thread for Heartbeat Timer")
        x = threading.Thread(target=self.IncrementHeartbeatTimer)
        self.heartbeat_timer_thread = x
        x.start()

    def IncrementHeartbeatTimer(self):
        logging.debug("Heartbeat Timer Started")
        with self.heartbeat_lock:
            self.heartbeat_start = perf_counter()
        nex = self.heartbeat_start

        while (nex - self.heartbeat_start) <= self.heartbeat_timeout and self.state == 4:
            nex = perf_counter()
            if (nex - self.heartbeat_start) >= self.heartbeat_timeout:
                logging.debug("Heartbeat Timeout, Starting Requests")
                retAcks = [0 for _ in self.node_list]
                for node in self.node_list:
                    logging.debug(f"{node[0]} {node[1]}")
                    if(node[0] == self.id):
                        continue
                    _ = threading.Thread(target=self.AppendEntriesReq, args=(node, retAcks))
                    _.start()
                #self.heartbeat_timer = 0
                logging.debug("Sent Requests after timeout")
                with self.heartbeat_lock:
                    #logging.debug("Setting heartbeat start "+str(self.heartbeat_start))
                    self.heartbeat_start = perf_counter()
                    #logging.debug("Setting heartbeat start "+str(self.heartbeat_start)+" "+str(nex))
            
        logging.debug("Exiting Heartbeat Timer")
    
    def _transitionToLeader(self):
        logging.debug("Transitioning to leader")
        self.state = LEADER
        #self.heartbeat_timer = self.heartbeat_timeout-1
        with self.next_lock:
            with self.logs_lock:
                for i in self.node_list:
                    self.next_index[i[0]] = len(self.logs)
        self.StartHeartbeatTimer()

    def _transitionToFollower(self):
        logging.debug("Transitioning to follower")
        self.state = FOLLOWER
        self.election_timer = 0
        self.StartElectionTimer()
        
    def _transitionToCandidate(self):
        logging.debug("Transitioning to candidate")
        self.state = CANDIDATE
        self.current_term += 1

    def VoteResponse(self, request):
        if request['term'] < self.current_term:
            return {
                "term": self.current_term,
                "voteGranted": False
            }
        else:
            if self.voted_for[0] == request["term"] :
                return {
                    "term": self.current_term,
                    "voteGranted": False
                }
            else:
                self.voted_for = (request["term"], request['candidate_id'])
                return {
                    "term": self.current_term,
                    "voteGranted": True
                }
            
    def _sendVoteReq(self, node,  message, votes):
        res = None
        tries = 0
        
        while not res and tries < 2:
            try:
                res = requests.post(f"http://localhost:{node[1]}/voteRequest", json=message)
                logging.debug("Boooooooo "+ str(res.json()))
                res.raise_for_status()
                res = res.json()
                if res['voteGranted']:
                    votes[node[0]] = 1
            except Exception as e:
                tries += 1
                logging.debug("Error contacting " + str(node[1]))
                logging.debug(str(e))

    def getAllBrokers(self):
        with self.conf_lock:
            data = {}
            f = open(self.conf_file, "r")
            if os.stat(self.conf_file).st_size != 0:
                data = json.load(f)
            f.close()
        if "RegisterBrokerRecord" in data.keys():
            return data["RegisterBrokerRecord"]["records"]
        else:
            return {}
    
    def getBroker(self, id: int):
        with self.conf_lock:
            data = {}
            f = open(self.conf_file, "r")
            if os.stat(self.conf_file).st_size != 0:
                data = json.load(f)
            f.close()
        if "RegisterBrokerRecord" in data.keys():
            records = data["RegisterBrokerRecord"]["records"]
        else:
            records = {}
        broker = {}
        for record in records:
            if record["brokerId"] == id:
                broker = record
        return broker
    
    def GetTopicRecord(self,TopicID):
        output = {}
        with self.conf_lock:
            data = {}
            f = open(self.conf_file, "r")
            if os.stat(self.conf_file).st_size != 0:
                data = json.load(f)
            f.close()
        if "TopicRecord" in data.keys():
            records = data["TopicRecord"]["records"]
        else:
            records = {}
        for record in records:
            if record["topicUUID"] == TopicID:
                output = record
        return output
        
    # def CreateTopicRecord(self,record): #timestamp to be added
    #     with self.conf_lock:
    #         data = {}
    #         f = open(self.conf_file, "r")
    #         if os.stat(self.conf_file).st_size != 0:
    #             data = json.load(f)

            
    #         data["TopicRecord"]["records"].append(record)
    #         f.close()
    #         f = open(self.conf_file,"w+")
    #         f.write(json.dumps(data))
    #         f.close()

    # def AddProducerIdRecord(self,record):#timestamp to be added
    #     with self.conf_lock:
    #         data = {}
    #         f = open(self.conf_file, "r")
    #         if os.stat(self.conf_file).st_size != 0:
    #             data = json.load(f)
    #         data["ProducerIdRecord"]["records"].append(record)
    #         f.close()
    #         f = open(self.conf_file,"w+")
    #         f.write(json.dumps(data))
    #         f.close()

    # def AddPartitionRecord(self,record):#timestamp to be added
    #     with self.conf_lock:
    #         data = {}
    #         f = open(self.conf_file, "r")
    #         if os.stat(self.conf_file).st_size != 0:
    #             data = json.load(f)
    #         f.close()
    #         data["PartitionRecord"]["records"].append(record)
    #         f = open(self.conf_file,"w+")
    #         f.write(json.dumps(data))
    #         f.close()
    
    
    
            
    
    def WriteToConfig(self, start, end):
        with self.logs_lock and self.conf_lock:
            config_data = {}
            f2 = open(self.conf_file, "r")
            if os.stat(self.conf_file).st_size != 0:
                config_data = json.load(f2)
            f2.close()

            #line = self.logs[self.commit_index-1]
            for idx in range(start, end):
                line = self.logs[idx]
                data = json.loads(line) 
                if data["name"] == "RegistrationChangeBrokerRecord" and "RegisterBrokerRecord" in config_data.keys():
                    for i,record in enumerate(config_data["RegisterBrokerRecord"]["records"]):
                        if record["brokerId"] == data["fields"]["brokerId"]:
                            config_data["RegisterBrokerRecord"]["records"][i] = data["fields"]
                            config_data["RegisterBrokerRecord"]["timestamp"][i] = data["timestamp"]

                elif data["name"] == "AddReplica":
                    replicas = data["fields"]["addingReplicas"]
                    for i,record in enumerate(config_data["PartitionRecord"]["records"]):
                        if record["partitionId"] == data["fields"]["partitionId"]:
                            logging.debug(config_data["PartitionRecord"]["records"][i])
                            config_data["PartitionRecord"]["records"][i]["replicas"].extend(replicas)
                            config_data["PartitionRecord"]["records"][i]["partitionEpoch"] += 1

                elif data["name"] == "RemoveReplica":
                    remReplicas = data["fields"]["removingReplicas"]
                    for i,record in enumerate(config_data["PartitionRecord"]["records"]):
                        if record["partitionId"] == data["fields"]["partitionId"]:
                            for replica in remReplicas:
                                if replica in config_data["PartitionRecord"]["records"][i]["replicas"]:
                                    config_data["PartitionRecord"]["records"][i]["replicas"].remove(replica)

                            config_data["PartitionRecord"]["records"][i]["partitionEpoch"] += 1


                else:
                    if data["name"] not in config_data.keys():
                        config_data[data["name"]] = {"records":[], "timestamp": []}
                    config_data[data["name"]]["records"].append(data["fields"])
                    config_data[data["name"]]["timestamp"].append(data["timestamp"])

            f = open(self.conf_file, "w+")
            f.write(json.dumps(config_data, indent=4))
            f.close()


        #RegisterBrokerRecord: register
        #TopicRecord: create topic
        #PartitionRecord: 
                        #create partition 
                        # remove a replica; increment the epoch on update;
                        #add a replica to partition; increment the epoch on update
        #ProducerIdsRecord: register a producer from a broker
        #BrokerRegistrationChangeBrokerRecord:
                        # updates to broker; increment the epoch on changes;
                        #unregister a broker
        
        #BrokerMgmt: a route takes previous offset/timestamp and returns metadata updates since then / if later than 10 minutes send entire snapshot; send diff of all metadata that has been updated
        #ClientMgmt: a route takes previous offset/timestamp and returns metadata updates since then / if later than 10 minutes send entire snapshot; send only topics, partitions and broker info

