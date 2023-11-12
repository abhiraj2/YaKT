from fastapi import FastAPI, Request
import uvicorn
import argparse
from raft.node import Node
import threading


parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', required=True)
parser.add_argument('-nl', '--nodes', default="./nodes.txt")
args = parser.parse_args()

nl = open(args.nodes, "r+")
node_list = []
for i, line in enumerate(nl.readlines()):
    line = line.strip()
    if(int(line) == int(args.port)):
        continue 
    node_list.append((i, int(line)))


node = Node(node_list)
node.StartTimer()


app = FastAPI()


@app.get("/hello")
def hello():
    return {
        "hello":"TAs"
    }

@app.post("/appendEntries")
async def appendEntries(message: Request):
    message = await message.json()
    print(message)
    node.timer = 0
    return {
        "appendEntries":  True
    }


@app.post("/registerBroker")
async def registerBroker(record: Request):
    parsed  = await record.json()
    print(parsed)
    res = node.AppendLogEntries(parsed['fields'])
    return res



uvicorn.run(app, host="localhost", port=int(args.port))    
        