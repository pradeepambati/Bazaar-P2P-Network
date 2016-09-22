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
from tempfile import NamedTemporaryFile
import shutil
import csv_operations
import os.path

# Multi-Threaded RPC Server.
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass

class LamportClock:
    #: The clocks current value.
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value

    def adjust(self, other):
        self.value = max(self.value, other)

    def forward(self):
        self.value += 1
        return self.value

# Peer
class peer:
    def __init__(self,host_addr,peer_id,neighbors,db):
        self.host_addr = host_addr
        self.peer_id = peer_id
        host_ip = socket.gethostbyname(socket.gethostname())
        self.db_server = host_ip + ':8057'
        self.neighbors = neighbors
        self.db = db 
        self.trader = []
       
        # Shared Resources
        self.didReceiveOK = False # Flag
        self.didReceiveWon = False # Flag
        self.didSendWon = False
        self.trade_list = {} 
        
        self.worker = self.create()
        # For Traders only.
        self.heartbeat_reply = False
        # Lamport Clock.
        self.lamport_clock = LamportClock()
       
        # Semaphores 
        self.flag_won_semaphore = td.BoundedSemaphore(1) 
        self.trade_list_semaphore = td.BoundedSemaphore(1) # Traders : Semaphore for the list of the trading products available.
        self.semaphore = td.BoundedSemaphore(1) # Semaphore for Altering the List of the traders.
        self.clock_semaphore = td.BoundedSemaphore(1)  # Semaphore For Clock. 
        self.heartbeat_reply_semaphore = td.BoundedSemaphore(1)     
        
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
        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        host_ip = socket.gethostbyname(socket.gethostname())
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.election_message,'election_message')
        server.register_function(self.register_products,'register_products')
        server.register_function(self.adjust_buyer_clock,'adjust_buyer_clock')
        server.register_function(self.election_restart_message,'election_restart_message')
        server.register_function(self.sync_cache,'sync_cache')
        server.register_function(self.periodic_ping_message,'periodic_ping_message')
        server.register_function(self.periodic_ping_reply,'periodic_ping_reply')
        server.register_function(self.trader_status_update,"trader_status_update")
        server.serve_forever()
   
    # election_restart_message : Once a peer recieves this messages, it indicates that a new election will be started.
    # Subsequent action is to set election running flag up.
    # This flag indicates to buyer who wants to buy, to wait till the election to restart the buying process.
    def election_restart_message(self):
        self.flag_won_semaphore.acquire()
        self.didReceiveOK = False
        self.didReceiveWon = False
        if self.peer_id == 1:
            time.sleep(3.0)
            thread = td.Thread(target=self.start_election,args=()) # Second Election
            thread.start()
        self.flag_won_semaphore.release()
        
    # Helper method : To send election restart messages to a peer on a new thread.      
    def send_restart_election_messages(self,_,neighbor):
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.election_restart_message() 
            
     # Helper method : To send election message to a peer on a new thread.                
    def send_message(self,message,neighbor):
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.election_message(message,{'peer_id':self.peer_id,'host_addr':self.host_addr,'status':1})
            
    # Helper method : To send the flags and send the "I won" message to peers.
    def fwd_won_message(self):
        print  "Dear buyers and sellers, My ID is ",self.peer_id, "and I am the new coordinator"
        self.didReceiveWon = True
        
        self.trader.append({'peer_id':self.peer_id,'host_addr':self.host_addr,'status' : 1})
        self.db['Role'] = 'Trader'
        self.flag_won_semaphore.release()
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.send_message,args=("I won",neighbor)) # Start Server
            thread.start() 
       
        if len(self.trader) == 1:
             time.sleep(3.0)
             for neighbor in self.neighbors:
                thread = td.Thread(target = self.send_restart_election_messages,args = (" ",neighbor))
                thread.start() # Sending Neighbors reelection notification.
        else:
            print "Begin Trading"
            thread2 = td.Thread(target=peer_local.begin_trading,args=())
            thread2.start()                 
            
    # election_message: This method handles three types of messages:
    # 1) "election" : Upon receiving this message, peer will reply to the sender with "OK" message and if there are any higher peers, forwards the message and waits for OK messages, if it doesn't receives any then its the leader.
    # 2) "OK" : Drops out of the election, sets the flag didReceiveOK, which prevents it from further forwading the election message.
    # 3) "I won": Upon receiving this message, peer sets the leader details to the variable trader and starts the trading process. 
    def election_message(self,message,neighbor):
        if message == "election":
            # Fwd the election to higher peers, if available. Response here are Ok and I won.
            if self.didReceiveOK or self.didReceiveWon:
                thread = td.Thread(target=self.send_message,args=("OK",neighbor)) # Start Server
                thread.start()
            else:
                thread = td.Thread(target=self.send_message,args=("OK",neighbor)) # Start Server
                thread.start()
                peers = [x['peer_id'] for x in self.neighbors]
                peers = np.array(peers)
                x = len(peers[peers > self.peer_id])

                if x > 0:
                    self.flag_won_semaphore.acquire()
                    self.isElectionRunning = True # Set the flag
                    self.flag_won_semaphore.release()
                    self.didReceiveOK = False
                    for neighbor in self.neighbors:
                        if neighbor['peer_id'] > self.peer_id:
                            if self.trader != [] and neighbor['peer_id'] == self.trader[0]['peer_id']:
                                pass
                            else:    
                                thread = td.Thread(target=self.send_message,args=("election",neighbor)) # Start Server
                                thread.start()
                                
                    time.sleep(2.0) # Timeout.
                                            
                    self.flag_won_semaphore.acquire()
                    if self.didReceiveOK == False and self.didSendWon == False:
                        self.didSendWon = True
                        self.fwd_won_message() # Release of semaphore is done by that method.
                    else:
                        self.flag_won_semaphore.release()
                               
                elif x == 0:
                    self.flag_won_semaphore.acquire()
                    if self.didSendWon == False:
                        self.didSendWon = True
                        self.fwd_won_message()
                    else:
                        self.flag_won_semaphore.release()
                        
        elif message == 'OK':
            # Drop out and wait
            self.didReceiveOK = True
                            
        elif message == 'I won':
            print "Peer ",self.peer_id,": Election Won Msg Received"
            self.flag_won_semaphore.acquire()
            self.didReceiveWon = True
            self.flag_won_semaphore.release()
            self.trader.append(neighbor)
            time.sleep(3.0)
            if len(self.trader) == 2: # Once Both the traders are elected, start the trading process.
                thread2 = td.Thread(target=peer_local.begin_trading,args=())
                thread2.start()
    
    # start_election: This methods starts the election by forwading election message to the peers, if there are no higehr peers, then its the leader and sends the "I won" message to the peers.        
    def start_election(self):
        print "Peer ",self.peer_id,": Started the election"

        time.sleep(1)
        # Check number of peers higher than you.
        peers = [x['peer_id'] for x in self.neighbors]
        peers = np.array(peers)
        x = len(peers[peers > self.peer_id])
        if x > 0:
            self.didReceiveOK = False
            self.didReceiveWon = False
            for neighbor in self.neighbors:
                if neighbor['peer_id'] > self.peer_id:
                    if self.trader != [] and neighbor['peer_id'] == self.trader[0]['peer_id']: # Don't send it to previous trader as he left the position.
                        pass
                    else:    
                        thread = td.Thread(target=self.send_message,args=("election",neighbor)) # Start Server
                        thread.start()  
            time.sleep(2.0)
            self.flag_won_semaphore.acquire()
            if self.didReceiveOK == False and self.didReceiveWon == False:
               self.didSendWon = True
               self.fwd_won_message()
            else:
                self.flag_won_semaphore.release()
        else: # No higher peers
            self.flag_won_semaphore.acquire()
            self.didSendWon = True
            self.fwd_won_message() # Release of semaphore is in fwd_won_message
     
    # Helper Method : Get the active Trader, checks the status of the trader and returns active trader proxy. 
    def get_active_trader(self):
        self.semaphore.acquire()
        x = random.randint(0, 1)
        trader = self.trader[x]
        if not trader["status"]:
            z = [0,1]
            z.remove(x)
            x = z[0]
            trader = self.trader[x]
        self.semaphore.release()
        return self.get_rpc(self.trader[x]["host_addr"])
         
    # begin_trading : For a seller, through this method they register there product at the trader. For buyer, they start lookup process for the products needed, in this lab every lookup process is directed at the trader and he sells those goods on behalf of the sellers.            
    def begin_trading(self):
        # Delay so that al the election message are replied or election is dropped by peers other than the trader.
        # Reset the flags.
        self.didReceiveWon = False
        self.didReceiveOK = False
        # If Seller, register the poducts.
        if self.db["Role"] == "Seller":
            connected,proxy = self.get_active_trader()
            p_n = None
            p_c = None
            for product_name, product_count in self.db['Inv'].iteritems():
                if product_count > 0:
                    p_n= product_name
                    p_c = product_count
            seller_info = {'seller_id': {'peer_id':self.peer_id,'host_addr':self.host_addr},'product_name':p_n,'product_count':p_c} 
            if connected:
                proxy.register_products(seller_info)
        elif self.db["Role"] == "Trader":
            connected,proxy = self.get_rpc(self.db_server)
            if connected: # Register with DB.
                proxy.register_traders({'peer_id':self.peer_id,'host_addr':self.host_addr})
            self.worker.start()

        # If buyer, wait for 2 sec for seller to register products and then start buying.
        elif self.db["Role"] == "Buyer":
            time.sleep(3.0 + self.peer_id/10.0) # Allow sellers to register the products.
            
            while len(self.db['shop'])!= 0:
                item = self.db['shop'][0]
                connected,proxy = self.get_active_trader()
                if connected:
                    self.clock_semaphore.acquire()
                    self.lamport_clock.forward()
                    request_ts = self.lamport_clock.value
                    #print "Buy Event",self.lamport_clock.value,self.peer_id
                    self.broadcast_lamport_clock()
                    self.clock_semaphore.release()
                    print "Peer ",self.peer_id, ": Requesting ",item
                    proxy.lookup({'peer_id':self.peer_id,'host_addr':self.host_addr},item,request_ts)       
                    self.db['shop'].remove(item)                   
                    time.sleep(3.0)           
            
    # broadcast_lamport_clock : This method broadcasts a peer's clock to all the peers.
    def broadcast_lamport_clock(self):
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.send_broadcast_message,args=("I won",neighbor)) # Start Server
            thread.start()
            
    def send_broadcast_message(self,_,neighbor): # Broadcast the clock.
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.adjust_buyer_clock(self.lamport_clock.value) 
    
    # adjust_buyer_clock: Upon receiving this message, a peer adjusts its clock, only buyer and seller adjust there clock, but not the trader.(This is a design choice.)
    def adjust_buyer_clock(self, other):
        if self.db["Role"] == "Buyer" or self.db["Role"] != "Trader": # Trader should not adjust his clock until he recives the lookup request.
            self.clock_semaphore.acquire()
            self.lamport_clock.adjust(other) 
            self.clock_semaphore.release()
            #print  peer_id," ", self.lamport_clock.value
    
    # register_products: Trader registers the seller goods.
    def register_products(self,seller_info): # Trader End.
        seller_peer_id = seller_info['seller_id']['peer_id'] # Key in trade-list
        self.trade_list[str(seller_peer_id)] = seller_info # Add the product in local cache and contact DB to update this info.
        connected,proxy = self.get_rpc(self.db_server)
        if connected:
            proxy.register_products(seller_info,{'peer_id':self.peer_id,'host_addr':self.host_addr})
    
    # lookup : Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.     
    def lookup(self,buyer_id,product_name,buyer_clock):
        self.lamport_clock.adjust(buyer_clock)        
        seller_list = []
        transaction_file_name =  "transactions_" + str(self.peer_id) + ".csv"
        for peer_id,seller_info in self.trade_list.iteritems():
            if seller_info["product_name"] == product_name: # Find all the seller who sells the product
                seller_list.append(seller_info)
                
        if len(seller_list) > 0:
            # Log the request
            seller = seller_list[0] # Choose the first seller.
            transaction_log = {str(self.lamport_clock.value) : {'product_name' : product_name, 'buyer_id' : buyer_id, 'seller_id':seller['seller_id'],'completed':False}}
            csv_operations.log_transaction(transaction_file_name,transaction_log) # Log the transaction.
            
            connected, proxy = self.get_rpc(self.db_server)
            if connected: # Contact DB Server for the transaction to complete.
                proxy.transaction(product_name,seller)

            # Reply to buyer that transaction is succesful. 
            connected,proxy = self.get_rpc(buyer_id["host_addr"])
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(product_name,seller['seller_id'],buyer_id,self.peer_id)
                
            connected,proxy = self.get_rpc(seller['seller_id']["host_addr"])
            if connected:# Pass the message to seller that its product is sold
                proxy.transaction(product_name,seller['seller_id'],buyer_id,self.peer_id)
             
            # Relog the request as done
            csv_operations.mark_transaction_complete(transaction_file_name,transaction_log,str(buyer_clock))

    # transaction : Seller just deducts the product count, Buyer prints the message.    
    def transaction(self, product_name, seller_id, buyer_id,trader_peer_id): # Buyer & Seller
        if self.db["Role"] == "Buyer":
            print  "Peer ", self.peer_id, " : Bought ",product_name, " from peer: ",seller_id["peer_id"]," via ",trader_peer_id
        elif self.db["Role"] == "Seller":
            self.db['Inv'][product_name] = self.db['Inv'][product_name] - 1
            #print "Sold ", product_name, " to peer: ",buyer_id["peer_id"]     
            if self.db['Inv'][product_name] == 0:
                # Pickup a random item and register that product with trader.
                product_list = ['Fish','Salt','Boar']
                y = random.randint(0, 2)
                random_product = product_list[y]
                self.db['Inv'][random_product] = 3
                seller_info = {'seller_id': {'peer_id':self.peer_id,'host_addr':self.host_addr},'product_name':random_product,'product_count':3}
                connected,proxy = self.get_active_trader() 
                if connected: 
                    proxy.register_products(seller_info)
                    
    # Sync Cache
    def sync_cache(self,seller_info):
        self.trade_list_semaphore.acquire()
        seller_peer_id = seller_info['seller_id']['peer_id']
        self.trade_list[str(seller_peer_id)] = seller_info 
        self.trade_list_semaphore.release()   
        
    def create(self):
        #"create looping call to send ping request"
        thr = td.Thread(target=self.periodic_ping_timer) # thread for the heartbeat protocol.
        return thr
        
    # Heart Beat Protocol.
    def periodic_ping_timer(self):
        other_trader = [trader for trader in self.trader if not trader['peer_id'] == self.peer_id][0]
        stop_condition = False # Intial Stop condition.
        while(stop_condition == False):
            self.heartbeat_reply = False # Set the reply flag as false
            connected,proxy = self.get_rpc(other_trader["host_addr"])
            if connected:
                proxy.periodic_ping_message({'peer_id':self.peer_id,'host_addr':self.host_addr,'status':1})            
            time.sleep(2.0)  # Time-out.
            self.heartbeat_reply_semaphore.acquire()
            if not self.heartbeat_reply: # Check whether the reply is received.
                self.heartbeat_reply_semaphore.release()
                stop_condition = True
            else:
                self.heartbeat_reply_semaphore.release()
                time.sleep(5.0)
                
        for neighbor in self.neighbors:# Trader is down. Broadcast to all the peers.
            connected,proxy = self.get_rpc(neighbor['host_addr'])
            if connected:
                proxy.trader_status_update(False,other_trader) # Trader is down.
                
        # Read the log of other trader and return un resolved requests.
        file_name = "transactions_" + str(other_trader['peer_id']) + ".csv"
        if os.path.isfile(file_name):
            unserved_requests = csv_operations.get_unserved_requests(file_name)
            if unserved_requests is None:
                print "None"
                pass
            else:
                for unserved_request in unserved_requests:
                    k,v  = unserved_request.items()[0]
                    self.lookup(v['buyer_id'],v['product_name'],int(k))
                         
    def trader_status_update(self,status,trader):
        self.semaphore.acquire()
        for x in xrange(len(self.trader)):
            if self.trader[x]['peer_id'] == trader['peer_id']:
                self.trader[x]['status'] = False # Update the status of the trader as false.
        self.semaphore.release()
        print "Status of ",trader['peer_id'],"is ",status
    
    # Helper Method: Sends the ping to the other trader.                        
    def periodic_ping_message(self,trader_info):
        connected,proxy = self.get_rpc(trader_info["host_addr"])
        if connected:
            proxy.periodic_ping_reply({'peer_id':self.peer_id,'host_addr':self.host_addr,'status':1})
        
    def periodic_ping_reply(self,trader_info):
        self.heartbeat_reply_semaphore.acquire()
        self.heartbeat_reply = True # Heartbeat reply
        self.heartbeat_reply_semaphore.release()
        
if __name__ == "__main__":
    host_ip = socket.gethostbyname(socket.gethostname())
    host_addr = host_ip + ":" + sys.argv[2]
    peer_id = int(sys.argv[1])
    db = json.loads(sys.argv[3])
    num_peers = int(sys.argv[4])
    
    # Computing Neigbors
    peer_ids = [x for x in xrange(1,num_peers+1)]
    host_ports = [(10007 + x) for x in xrange(0,num_peers)]
    host_addrs = [(host_ip + ':' + str(port)) for port in host_ports]
    neighbors = [{'peer_id':p,'host_addr':h} for p,h in zip(peer_ids,host_addrs)]
    neighbors.remove({'peer_id':peer_id,'host_addr':host_addr})
    
    #Declare a peer variable and start it.  
    peer_local = peer(host_addr,peer_id,neighbors,db)
    thread1 = td.Thread(target=peer_local.startServer,args=()) # Start Server
    thread1.start()    
    # Starting the election, lower peers.
    if peer_id <= 2:
        thread1 = td.Thread(target=peer_local.start_election,args=()) # Start Server
        thread1.start()