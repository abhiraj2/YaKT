from fastapi import FastAPI, Request
import uvicorn
import argparse
from raft.node import Node
import threading
from time import sleep

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', required=True)
parser.add_argument('-nl', '--nodes', default="./nodes.txt")
parser.add_argument('-s', '--start', required=True)
args = parser.parse_args()

nl = open(args.nodes, "r+")
node_list = []
s_id = -1
for i, line in enumerate(nl.readlines()):
    line = line.strip()
    if(int(line) == int(args.port)):
        s_id = i
    node_list.append((i, int(line)))

#sleep(int(args.start))

app = FastAPI()

node = Node(node_list, s_id)


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
    print(message)
    node._transitionToFollower()
    res = node.AppendEntriesRes(message)
    return res


@app.post("/registerBroker")
async def registerBroker(record: Request):
    parsed  = await record.json()
    print(parsed)
    res = node.AppendLogEntries(parsed['fields'])
    return res


@app.post("/voteRequest")
async def voteRequest(record: Request):
    parsed = await record.json()
    res = node.VoteResponse(parsed)
    return res

app_thread = threading.Thread(target=StartApplication, args=(app, int(args.port)))
app_thread.start()
node.StartElectionTimer()

app_thread.join()
        