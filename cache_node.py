class CacheNode:
    def __init__(self, id, addr):
        self.id = id
        self.addr = addr

    def __str__(self):
        return "Cache Node {} - addr: {}".format(self.id, self.addr)
    
    def __repr__(self):
        return str(self)