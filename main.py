from fastapi import FastAPI, Request
import uvicorn
import argparse
from raft.node import Node
import threading
from time import sleep, process_time
import os

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', required=True)
parser.add_argument('-nl', '--nodes', default="./nodes.txt")
parser.add_argument("-f", "--file", required=True)
parser.add_argument("-c", "--config", required=True)
args = parser.parse_args()


log_file = args.file
read_logs = []
if os.path.isfile(log_file):
    f = open(log_file, "r+")
    for line in f.readlines():
        line = line.strip()
        read_logs.append(line)
    f.close()
else:
    f = open(log_file, "w+")
    f.close()

if not os.path.isfile(args.config):
    f_config = open(args.config, "w+")
    f_config.close()

nl = open(args.nodes, "r+")
node_list = []
s_id = -1
for i, line in enumerate(nl.readlines()):
    line = line.strip()
    if(int(line) == int(args.port)):
        s_id = i
    node_list.append((i, int(line)))
nl.close()
#sleep(int(args.start))

app = FastAPI()

node = Node(node_list, s_id, read_logs, log_file, args.config)


def StartApplication(app, port):
    uvicorn.run(app, host="localhost", port=port)


@app.get("/hello")
def hello():
    return {
        "hello":"TAs"
    }

@app.post("/appendEntries")
async def appendEntries(message: Request):
    message = await message.json()
    node.election_start = process_time()
    res = node.AppendEntriesRes(message)
    return res


@app.post("/registerBroker")
async def registerBroker(record: Request):
    parsed  = await record.json()
    print(parsed)
    res = node.AppendLogEntries(parsed)
    return res

@app.get("/getBrokerByID/{id}")
async def getBrokerByID(id):
    res = node.getBroker(id)
    return res

@app.post("/voteRequest")
async def voteRequest(record: Request):
    parsed = await record.json()
    res = node.VoteResponse(parsed)
    return res

@app.post("/topicRecord")
async def AddTopicRecord(record:Request):
    parsed = await record.json()
    res = node.AppendLogEntries(parsed)
    return res

@app.get("/topicRecord/{TopicID}")
async def GetTopicRecord(TopicID):
    res = node.GetTopicRecord(TopicID)
    return res

app_thread = threading.Thread(target=StartApplication, args=(app, int(args.port)))
app_thread.start()
node.StartElectionTimer()

app_thread.join()
        