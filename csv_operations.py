from tempfile import NamedTemporaryFile
import shutil
import csv
import json

# Log a transaction
def log_transaction(filename,log):
    log = json.dumps(log)
    with open(filename,'a') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        csvWriter.writerow([log])
        
# Mark Transaction Complete        
def mark_transaction_complete(filename,transaction,identifier):
    tempfile = NamedTemporaryFile(delete=False)
    with open(filename, 'rb') as csvFile,tempfile:
        reader = csv.reader(csvFile, delimiter=' ')
        writer = csv.writer(tempfile, delimiter=' ')
        for row in reader:
            row = json.loads(row[0])
            k,_ = row.items()[0]
            if k == identifier:
                row[k]['completed'] = True
            row = json.dumps(row)
            writer.writerow([row])
    shutil.move(tempfile.name, filename)  
    
# Log the seller info 
def seller_log(trade_list):
    with open('seller_info.csv','wb') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        for k,v in trade_list.iteritems():
            log = json.dumps({k:v})
            csvWriter.writerow([log])  
            
# Read the seller log           
def read_seller_log():
    with open('seller_info.csv','rb') as csvF:
        seller_log = csv.reader(csvF,delimiter = ' ')
        dictionary = {}
        for log in seller_log:
            log = json.loads(log[0])
            k,v = log.items()[0]
            dictionary[k] = v
    return dictionary

# Mark Transaction Complete        
def change_entry(transaction,identifier):
    tempfile = NamedTemporaryFile(delete=False)
    with open('seller_info.csv', 'rb') as csvFile,tempfile:
        reader = csv.reader(csvFile, delimiter=' ')
        writer = csv.writer(tempfile, delimiter=' ')
        for row in reader:
            row = json.loads(row[0])
            k,_ = row.items()[0]
            if k == identifier:
                row = transaction
            row = json.dumps(row)
            writer.writerow([row])
    shutil.move(tempfile.name, 'seller_info.csv')

# Return any unserved requests.    
def get_unserved_requests(file_name):
    with open(file_name,'rb') as csvF:
        transaction_log = csv.reader(csvF,delimiter = ' ')
        open_requests = []
        transaction_list = list(transaction_log)
        last_request = json.loads(transaction_list[len(transaction_list)-1][0])
        _,v = last_request.items()[0]
        if v['completed'] == False:
            return last_request
        else:
            return None