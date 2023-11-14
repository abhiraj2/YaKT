from uuid import uuid4
from random import randint
import threading
import logging
import requests
from time import process_time
MAX_TIME = 50
MIN_TIME = 20

logging.basicConfig(level=logging.DEBUG)

class Node:
    def __init__(self, server_list, s_id):
        # Persistent states, remain same across all terms
        self.id = s_id
        self.current_term = 0
        self.election_timeout = randint(MIN_TIME, MAX_TIME)
        logging.debug(self.election_timeout)
        self.heartbeat_timeout = 10
        
        self.voted_for = (None, None)
        self.logs = []
        self.node_list = []
        self.current_leader = None
        for node in server_list:
            self.node_list.append(node)

        # volatile
        self.state = 1 # one hot coding for follower(1), candidate(2), leader(4)
        self.commit_index = 0
        self.last_applied = 0
        self.election_timer_thread= None
        self.heartbeat_timer_thread = None
        self.election_start = 0
        self.heartbeat_start = 0

        # for leader, to be reinitialized
        self.next_index = [0 for _ in range(len(self.node_list))]
        self.match_index = [0 for _ in range(len(self.node_list))]


    # RPC to send to follower, run this on new thread and wait for response.
    # If no response send again till u get a response.
    def AppendEntriesReq(self, follower, retAcks):
        logging.debug("Starting Thread for Append RPC from leader to follower " + str(follower[0]))
        message = {
            "term": self.current_term,
            "leader_id": str(self.id),
            "prevLogIndex": self.next_index[follower[0]]-1 if self.next_index[follower[0]]-1 >= 0 else 0,
            "prevLogTerm": self.logs[self.next_index[follower[0]]-1] if self.next_index[follower[0]]-1 >= 0 else "NULL",
            "entries": self.logs[self.next_index[follower[0]]:],
            "leaders_commit": self.commit_index
        }
        
        retAcks[follower[0]] = 1 
        logging.debug("Sending Request to " + str(follower[1]) + " " + str(follower[0]))
        res = {
        }
        while 'success' not in res.keys():
            logging.debug("Trying Append")
            try:
                res = requests.post(f"http://localhost:{follower[1]}/appendEntries", json=message)
                res.raise_for_status()
                res = res.json()
                logging.debug("Response "+ str(res))
            except Exception as e:
                logging.debug("Error contacting " + str(follower[1]))
                logging.debug(str(e))
                continue
        
        if not res["success"]:
            if res["term"] > self.current_term:
                self._transitionToFollower()
            else:
                if self.next_index[follower[0]] > 0:
                    self.next_index[follower[0]] -=  1
                    self.AppendEntriesReq(follower, retAcks)
        else:
            logging.debug("Stopping Thread for Append RPC from leader to follower " + str(follower[0]))
            return res

    def AppendEntriesRes(self, message):
        if message['term'] < self.current_term :
            return {
                "term": self.current_term,
                "success": False
            }
        if message["prevLogTerm"] == "NULL":
            return {
                "term": self.current_term,
                "success": True   
            }
        if message['prevLogIndex'] >= len(self.logs) or message['prevLogTerm'] != self.logs[message["prevLogIndex"]]:
            return {
                "term": self.current_term,
                "success": False
            }
        else:
            for i, ele in enumerate(message['entries']):
                if i < len(self.logs):
                    self.logs[i] = ele
                else:
                    self.logs.append(ele)
        return {
                "term": self.current_term,
                "success": True
            }      

    def AppendLogEntries(self, message):
        if self.state != 4:
            return {
                "current_leader": self.current_leader,
                "success": False
            }
        else:
            self.logs.append(message)
            retAcks = [0 for _ in range(len(self.node_list))]
            for node in self.node_list:
                if(node[0] == self.id):
                        continue
                _ = threading.Thread(target=self.AppendEntriesReq, args=(node, retAcks))
                _.start()
            while sum(retAcks) <= len(self.node_list)/2:
                # logging.info("Sum " + str(sum(retAcks)))
                continue
            self.commit_index += 1

            return{
                "success": True
            }
        
    def StartElection(self):
        self.state <<= 1 # Move to candidate state
        self.voted_for = (self.current_term, self.id)
        message = {
            "term": self.current_term,
            "candidate_id": str(self.id),
            "lastLogIndex": len(self.logs),
            "lastLogTerm": self.logs[len(self.logs)-1] if len(self.logs) > 0 else "NULL",
        }

        votes = [0 for _ in self.node_list] 
        for node in self.node_list:
            if node[0] == self.id:
                continue
            _ = threading.Thread(target=self._sendVoteReq, args=(node, message, votes))
            _.start()
        
        while sum(votes) <= len(self.node_list)/2:
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
        while nex-self.election_start <= self.election_timeout and self.state != 4:
            if nex-self.election_start >= self.election_timeout:
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
        self.heartbeat_start = process_time()
        nex = self.heartbeat_start
        while (nex - self.heartbeat_start) < self.heartbeat_timeout+5 and self.state == 4:
            if (nex - self.heartbeat_start) >= self.heartbeat_timeout:
                retAcks = [0 for _ in self.node_list]
                for node in self.node_list:
                    logging.debug(f"{node[0]} {node[1]}")
                    if(node[0] == self.id):
                        continue
                    _ = threading.Thread(target=self.AppendEntriesReq, args=(node, retAcks))
                    _.start()
                #self.heartbeat_timer = 0
                self.heartbeat_start = process_time()
            nex = process_time()
        logging.debug("Exiting Heartbeat Timer")
    
    def _transitionToLeader(self):
        self.state = 4
        #self.heartbeat_timer = self.heartbeat_timeout-1
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