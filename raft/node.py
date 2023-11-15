from uuid import uuid4
import json
from random import randint
import threading
import logging
import requests
from time import process_time
MAX_TIME = 50
MIN_TIME = 20

logging.basicConfig(level=logging.DEBUG)

class Node:
    def __init__(self, server_list, s_id, read_logs, log_file):
        # Persistent states, remain same across all terms
        self.id = s_id
        self.current_term = 0
        self.election_timeout = randint(MIN_TIME, MAX_TIME)
        logging.debug(self.election_timeout)
        self.heartbeat_timeout = 10
        
        self.file_lock = threading.Lock()
        self.log_file = log_file
        
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
        self.state = 1 # one hot coding for follower(1), candidate(2), leader(4)
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
        
        retAcks[follower[0]] = 1 
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
                self.heartbeat_start = process_time()
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
            self.commit_index += 1
            with self.file_lock:
                f = open(self.log_file, "a+")
                with self.logs_lock:
                    f.write(self.logs[-1])
                    f.write("\n")
                f.close()
            return{
                "success": True
            }
        
    def StartElection(self):
        logging.debug("Starting Election")
        self.state <<= 1 # Move to candidate state
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
        
        start = process_time()
        while sum(votes) <= len(self.node_list)/2 and (process_time() - start) <= 20:
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
        self.election_start = process_time()
        nex = self.election_start
        while (nex-self.election_start) <= self.election_timeout and self.state != 4:
            if (nex-self.election_start) >= self.election_timeout:
                self.StartElection()
            nex = process_time()
        logging.debug("Exiting Timer")
    
    def StartHeartbeatTimer(self):
        logging.debug("Starting new thread for Heartbeat Timer")
        x = threading.Thread(target=self.IncrementHeartbeatTimer)
        self.heartbeat_timer_thread = x
        x.start()

    def IncrementHeartbeatTimer(self):
        logging.debug("Heartbeat Timer Started")
        with self.heartbeat_lock:
            self.heartbeat_start = process_time()
        nex = self.heartbeat_start
        while (nex - self.heartbeat_start) <= self.heartbeat_timeout and self.state == 4:
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
                    self.heartbeat_start = process_time()
            nex = process_time()
        logging.debug("Exiting Heartbeat Timer")
    
    def _transitionToLeader(self):
        self.state = 4
        self.current_term += 1
        #self.heartbeat_timer = self.heartbeat_timeout-1
        with self.next_lock:
            with self.logs_lock:
                for i in self.node_list:
                    self.next_index[i[0]] = len(self.logs)
        self.StartHeartbeatTimer()

    def _transitionToFollower(self):
        self.state = 1
        self.election_timer = 0
        self.StartElectionTimer()
        

    def VoteResponse(self, request):
        if request['term'] < self.current_term:
            return {
                "term": self.current_term,
                "voteGranted": False
            }
        else:
            if self.voted_for[0] == self.current_term :
                return {
                    "term": self.current_term,
                    "voteGranted": False
                }
            else:
                self.voted_for = (self.current_term, request['candidate_id'])
                return {
                    "term": self.current_term,
                    "voteGranted": True
                }
            
    def _sendVoteReq(self, node,  message, votes):
        res = None
        while not res:
            try:
                res = requests.post(f"http://localhost:{node[1]}/voteRequest", json=message)
                logging.debug("Boooooooo "+ str(res.json()))
                res.raise_for_status()
                res = res.json()
                if res['voteGranted']:
                    votes[node[0]] = 1
            except Exception as e:
                logging.debug("Error contacting " + str(node[1]))
                logging.debug(str(e))