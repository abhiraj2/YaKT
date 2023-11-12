from uuid import uuid4
from random import randint
import threading
import logging
MAX_TIME = 300
MIN_TIME = 150

logging.basicConfig(level=logging.INFO)

class Node:
    def __init__(self, server_list):
        # Persistent states, remain same across all terms
        self.id = uuid4()
        self.current_term = 0
        self.election_timeout = randint(MIN_TIME, MAX_TIME)
        self.heartbeat_time = 75
        
        self.voted_for = None
        self.logs = []
        self.node_list = []
        self.current_leader = None
        for i in server_list:
            self.node_list.append(i)

        # volatile
        self.state = 1 # one hot coding for follower(1), candidate(2), leader(4)
        self.commit_index = 0
        self.last_applied = 0
        self.timer = 0

        # for leader, to be reinitialized
        self.next_index = [0 for i in range(len(self.node_list))]
        self.match_index = [0 for i in range(len(self.node_list))]


    # RPC to send to follower, run this on new thread and wait for response.
    # If no response send again till u get a response.
    def AppendEntriesReq(self, follower, retAcks):
        logging.info("Starting Thread for Append RPC from leader to follower " + str(follower[0]))
        message = {
            "term": self.current_term,
            "leader_id": self.id,
            "prevLogIndex": self.next_index[follower[0]]-1,
            "prevLogTerm": self.logs[self.next_index[follower[0]]-1],
            "entries": self.logs[self.next_index[follower[0]]:],
            "leaders_commit": self.commit_index
        }
        
        retAcks[follower[0]] = 1 
        # send message to follower, wait for response
        # if response is True, end
        # if false and leader term is less than in response, update term and go to follower state
        # if false, decrement next_index and retry the loop
        logging.info("Stopping Thread for Append RPC from leader to follower " + str(follower[0]))

    def AppendEntriesRes(self, message):
        if message.term < self.current_term :
            return {
                "term": self.current_term,
                "success": False
            }
        if message.prevLogTerm != self.logs[message.prevLogIndex]:
            return {
                "term": self.current_term,
                "success": False
            }
        else:
            for i, ele in enumerate(message.entries):
                if i < len(self.logs):
                    message.logs[i] = ele
                else:
                    message.logs.append(ele)
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
                x = threading.Thread(target=self.AppendEntriesReq, args=(node, retAcks))
                x.start()
            while sum(retAcks) <= len(self.node_list)/2:
                logging.info("Sum " + str(sum(retAcks)))
                continue
            self.commit_index += 1
            
            return{
                "success": True
            }
        
    def StartElection(self):
        self.state <<= 1 # Move to candidate state
        logging.info("Request for votes")
        self.state <<= 1 # Assume majority and move to leader

    def StartTimer(self):
        logging.info("Starting new thread for Timer")
        x = threading.Thread(target=self.IncrementTimer)
        x.start()

    def IncrementTimer(self):
        logging.info("Timer Started")
        while self.timer <= self.election_timeout and self.state != 4:
            self.timer += 1
            if self.timer == self.election_timeout:
                self.StartElection()
                self.timer = 0
        logging.info("Exiting Time")