MGR:
 - add_node(<cache_node_addr>)
 - get_key(k)
 - set_key(k, v)

CACHE NODE:
- set_capacity(new_capacity)
- get_key(k)
- set_key(k, v)
- replicate([{k1, k2, ...}, <addr>, {k3, k4, ...}, <addr>, ... ])
- startup(<mgr_addr>)