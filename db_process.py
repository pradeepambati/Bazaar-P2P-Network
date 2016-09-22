from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import time
import threading as td
import socket,SocketServer
import random
import numpy as np
import sys
import json
import csv
import csv_operations
import os.path
import csv_operations
# Multi-threaded RPC Server
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass

class database_process:

    def __init__(self,host_addr):
        self.host_addr = host_addr # Its Host Address.
        self.traders_info = [] # Initially Null.
        self.inventory_by_seller = {}
        self.semaphore = td.BoundedSemaphore(1)
        self.inventory_by_product = {}
        
    # Helper Method: Returns the proxy for specified address.
    def get_rpc(self,neighbor):
        a = xmlrpclib.ServerProxy('http://' + str(neighbor) + '/')
        try:
            a.test()   # Call a fictive method.
        except xmlrpclib.Fault:
            # connected to the server and the method doesn't exist which is expected.
            pass
        except socket.error:
            # Not connected ; socket error mean that the service is unreachable.
            return False, None
            
        # Just in case the method is registered in the XmlRPC server
        return True, a
   
    def register_traders(self,trader_info): # Register Traders.
        if not trader_info in self.traders_info:
            self.traders_info.append(trader_info)     
     
    def register_products(self,seller_info,trader_info):# External Function
        self.semaphore.acquire()
        seller_peer_id = seller_info['seller_id']['peer_id']
        self.inventory_by_seller[str(seller_peer_id)] = seller_info
        
        # Register this in inventory by product
        if seller_info['product_name'] in self.inventory_by_product:
            self.inventory_by_product[seller_info['product_name']]['product_count'] += seller_info['product_count']
        else:
            self.inventory_by_product[seller_info['product_name']] = {'product_count' : seller_info['product_count']}
        
        csv_operations.seller_log(self.inventory_by_seller)# Record This Info in a CSV File.
        
        # Push change to other trader.
        for trader in self.traders_info:
            if not trader['peer_id'] is trader_info['peer_id']:
                self.push_changes(trader,seller_info)
                
        self.semaphore.release()
    
    #Lookup: Returns the product count.            
    def lookup(self,product_name):
        count = 0
        self.semaphore.acquire()
        count = self.inventory_by_product[product_name] # Return the count of product.
        self.semaphore.release() 
        return count
        
    # Push the change to the respective trader.     
    def push_changes(self,trader_info,seller_info): 
        connected,proxy = self.get_rpc(trader_info['host_addr'])
        if connected:
            proxy.sync_cache(seller_info)
    
    # Transaction : Deduct the product and push the changes.
    def transaction(self,product_name,seller_info): 
        self.semaphore.acquire()
        seller_peer_id = str(seller_info['seller_id']['peer_id'])
        self.inventory_by_seller[seller_peer_id]["product_count"] = self.inventory_by_seller[seller_peer_id]["product_count"] -1
        self.inventory_by_product[product_name]['product_count'] -= 1
        csv_operations.change_entry(self.inventory_by_seller[seller_peer_id], seller_peer_id)
        for trader in self.traders_info:
            seller_peer_id = seller_info['seller_id']['peer_id']
            new_info = self.inventory_by_seller[str(seller_peer_id)]
            self.push_changes(trader,new_info)
        self.semaphore.release()
        
    def startServer(self):
        print "Started the server"
        host_ip = socket.gethostbyname(socket.gethostname())
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.register_products,'register_products')
        server.register_function(self.register_traders,'register_traders')
        server.serve_forever()
        
if __name__ == "__main__":
    host_ip = socket.gethostbyname(socket.gethostname())
    host_addr = host_ip + ":" + sys.argv[1]
    db_server = database_process(host_addr)
    thread = td.Thread(target=db_server.startServer,args=()) # Start Server
    thread.start()