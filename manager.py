import time
from flask import Flask, request, jsonify
import threading

import requests

from cache_node import CacheNode
from list_keys import list_keys

app = Flask(__name__)

cache_nodes = []
cache_addrs = {}

def make_replication_request_payload(to_dict, cache_nodes, new_cache_nodes):
    # to_dict = { toidx1: [k1, k2,...], toidx2: [k3, k4, ...] }
    replication_request_payload = {
        new_cache_nodes[to_idx].addr : keys for (to_idx, keys) in to_dict.items()
    }

    # { toaddr1: [k1, k2,...], toaddr2: [k3, k4, ...] }
    return replication_request_payload

def send_replication_requests(moves, cache_nodes, new_cache_nodes):
    # moves = { fromidx1: { toidx1: [k1, k2,...]}}
    for (from_index, to_dict) in moves.items():
        from_node = cache_nodes[from_index]
        replication_request_payload = make_replication_request_payload(to_dict, cache_nodes, new_cache_nodes)
        print("Sending replication request payload...")
        print(replication_request_payload)
        replication_request_response = requests.post("{}/replicate".format(from_node.addr), json=replication_request_payload)
        print("Got replication_request_response: ")
        print(replication_request_response)
        print(replication_request_response.status_code)
        if replication_request_response.status_code != 200:
            raise Exception("Unable to replicate the required keys from %s" % str(from_node))



def add_node_background_task(id, addr):
    global cache_nodes
    global cache_addrs

    print(f"Background task started")
    new_cache_node = CacheNode(id, addr)
    # cache_nodes.append(new_cache_node)
    # cache_addrs[new_cache_node_addr] = new_cache_node_id
    print("Before adding the new node...")
    print_data()
    new_cache_nodes = [cn for cn in cache_nodes]
    new_cache_nodes.append(new_cache_node)
    new_cache_addrs = {k:v for (k,v) in cache_addrs.items()}
    new_cache_addrs[addr] = id
    stays = []
    moves = {} # { fromidx1: { toidx1: [k1, k2,...]}}
    for k in list_keys(cache_nodes):
        current_hash = hash_key(k)
        new_hash = hash_key(k, new_cache_nodes)
        if new_hash == current_hash:
            stays.append((k, current_hash, new_hash))
            continue
        
        # new_hash != current_hash
        if current_hash not in moves:
            moves[current_hash] = dict()
        from_dict = moves[current_hash]
        if new_hash not in from_dict:
            from_dict[new_hash] = []
        to_array = from_dict[new_hash]
        to_array.append(k)
    print("Got moves: ")
    print(moves)
    print("Here is where we would go through and replicate all the data...")
    send_replication_requests(moves, cache_nodes, new_cache_nodes)

    cache_nodes = new_cache_nodes
    cache_addrs = new_cache_addrs


    print(f"Background task finished")
    print_data()

def hash_key(k, these_cache_nodes=None):
    if these_cache_nodes is None:
        these_cache_nodes = cache_nodes
    print("Inside hash_key, got called with these_cache_nodes: ")
    print(these_cache_nodes)
    k_num = int(k)
    hashed_key = k_num % len(these_cache_nodes)
    return hashed_key

def print_data():
    print("Inside add_node, now the cache nodes are: ")
    print(cache_nodes)
    print("Inside add_node, now the cache_addrs are: ")
    print(cache_addrs)
    return {
        "cache_nodes": [str(n) for n in cache_nodes],
        "cache_addrs": cache_addrs
    }


def get_target_node(k):
    i = hash_key(k)
    target_cache_node = cache_nodes[i]
    print("The target_cache_node is: ")
    print(target_cache_node)
    return target_cache_node

@app.route('/', methods=['GET'])
def health_check():
    return 'Hello from manager: {}'.format(time.time())

@app.route('/dump-data', methods=['GET'])
def dump_data():
    return jsonify(print_data())

@app.route('/add-node', methods=['POST'])
def add_node():
    data = request.get_json()
    print("Inside add_node, got POST body: ")
    print(data)
    new_cache_node_id = len(cache_nodes) + 1
    new_cache_node_addr = data['cache_node_addr'] # TODO: ADD POST BODY VALIDATION
    if new_cache_node_addr in cache_addrs:
        return jsonify({"success": False, "error": "Duplicate cache node address"}), 400
    #new_cache_node = CacheNode(new_cache_node_id, new_cache_node_addr)
    #cache_nodes.append(new_cache_node)
    #cache_addrs[new_cache_node_addr] = new_cache_node_id
    #print_data()
    thread = threading.Thread(target=add_node_background_task, args=(new_cache_node_id, new_cache_node_addr))
    thread.start()
    return jsonify({"success": True})

@app.route('/get-key/<k>', methods=['GET'])
def get_key(k):
    # i = hash_key(k)
    # target_cache_node = cache_nodes[i]
    # print("The target_cache_node is: ")
    # print(target_cache_node)
    target_cache_node = get_target_node(k)
    print("Now making a request to the target cache node...")
    cache_node_request = requests.get("{}/get-key/{}".format(target_cache_node.addr, k))
    print("Got response from cache node: ")
    print(cache_node_request)
    if cache_node_request.status_code == 404:
        return jsonify({
            "success": False,
            "error": "Not found"
        }), 404
    cached_data = cache_node_request.json()
    print(cached_data)
    return jsonify({
        "success": True, 
        "value": cached_data["value"],
        "source_node_id": target_cache_node.id
    })

@app.route('/set-key/<k>', methods=['POST'])
def set_key(k):
    target_cache_node = get_target_node(k)
    data = request.get_json()
    value = data["value"]
    print("Now making a request to set the key in the cache node: ")
    set_request = requests.post("{}/set-key/{}".format(target_cache_node.addr, k), json={
        "value": value
    })
    print("Got set_request: ")
    print(set_request)
    set_request_body = set_request.json()
    print("Got set_request body: ")
    print(set_request_body)
    if set_request.status_code != 200:
        return jsonify({"success": False, "error": set_request_body["error"]}), set_request.status_code
    return jsonify({"success": True, "key": k, "value": value, "source_node_id": target_cache_node.id})


if __name__ == "__main__":
    app.run()