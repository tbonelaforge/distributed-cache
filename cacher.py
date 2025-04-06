import time
from flask import Flask, request, jsonify


import sys

import requests








data = dict()

id = None
port = None

def create_app(id_spec, mgr_addr):
    app = Flask(__name__)
    id = int(id_spec)
    port = 5000 + id

    @app.route('/', methods=['GET'])
    def health_check():
        return 'Hello from cacher id {} - {}'.format(id, time.time())

    @app.route('/dump-data', methods=['GET'])
    def dump_data():
        return jsonify(data)

    @app.route('/get-key/<k>', methods=['GET'])
    def get_key(k):
        if k not in data:
            return jsonify({"success": False, "error": "Not Found"}), 404
        v = data[k]
        return jsonify({"success": True, "key": k, "value": v})
    
    @app.route('/set-key/<k>', methods=['POST'])
    def set_key(k):
        incoming_data = request.get_json()
        value = incoming_data["value"]
        data[k] = value
        return jsonify({"success": True, "key": k, "value": value})


    @app.route('/replicate', methods=['POST'])
    def replicate():
        payload = request.get_json()
        print("Inside replicate, got payload: ")
        print(payload)
        # payload = { toaddr1: [k1, k2,...], toaddr2: [k3, k4, ...] }

        for (to_addr, keys) in payload.items():
            for k in keys:
                set_key_response = requests.post("{}/set-key/{}".format(to_addr, k), json={
                    "value": data[k]
                })
                if set_key_response.status_code != 200:
                    raise Exception("Unable to set key %d in to_addr %s" % (k, to_addr))
        return jsonify({"success": True, "replicated": payload})



    with app.app_context() as ctx:
        print("Inside the manually push a context handler, got called with ctx...")
        print(ctx)
        print("do a request to the manager at %s here, and pass my id of: %d..." % (mgr_addr, id_spec))
        manager_response = requests.post("{}/add-node".format(mgr_addr), json={
            "cache_node_addr": "http://localhost:{}".format(port)
        })
        print("Got manager_response: ")
        print(manager_response)
        print("The json is: ")
        print(manager_response.json())
        if manager_response.status_code != 200:
            print("Cacher exiting, could not validate with manager")
            exit(1)
        

    return app







    


if __name__ == "__main__":
    

    if len(sys.argv) == 3:
        arguments = sys.argv[1:]
        print("Arguments:", arguments)
        id_spec = arguments[0]
        mgr_addr = arguments[1]
        print("Got specified id: ")
        print(id_spec)
        id = int(id_spec)
        my_port = 5000 + id
        # app.run(port=my_port)
        print("Running the manager, with port = %d" % (my_port))
        manager.run(port=my_port)
        print("Now making sure that the mgr has space for me...")
        response = requests.post(
            "{}/add-node".format(mgr_addr),
            json = {"cache_node_addr": "http://localhost:{}".format(my_port)}
        )
        print("The response from the manager was: ")
        print(response)

    else:
        print("Incorret arguments provided - try python cacher.py <id> <mgr_addr>")
        