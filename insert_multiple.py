from __future__ import print_function
import aerospike
from aerospike import exception as ex
import sys
import string
from multiprocessing import Process, Lock, Pool
import time
import configs

def closeClient (client):
	client.close()

def insertInAS(client,record,asParams):
    try:
        key = (asParams['namespace'], asParams['set'], record[configs.PK_field])
        client.put(key,record)
	print("Inserted", record[configs.PK_field])
    except ex.RecordError as e:
        print("Error: {} [{}]".format(e.msg, e.code))
    except Exception as e:
        import traceback
        print(traceback.print_exc())

def connectToAS(config, asParams):
    try:
        client = aerospike.client(config).connect(asParams['user_name'], asParams['password'])
        return client
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

def parseRecord():
	record = configs.json
	return record

def setParams():
	asParams={}
        asParams['user_name']=configs.user_name
        asParams["password"]=configs.password
        asParams["namespace"]=configs.namespace
        asParams["set"]=configs.set

        hosts = configs.hosts
	config = {'hosts': hosts}
	write_policies = {'total_timeout': 20000, 'max_retries': 10}
	read_policies = {'total_timeout': 15000, 'max_retries': 10}
	policies = {'write': write_policies, 'read': read_policies}
	config['policies'] = policies
	
	return config, asParams

def main():
	config, asParams = setParams()
	record = parseRecord()
	client = connectToAS(config, asParams)
	f = open("PK_list.txt","r")
	for line in f:
		pkey = str(line.strip())
		record[configs.PK_field]=pkey
		insertInAS(client,record,asParams)
	closeClient(client)
	
if __name__ == '__main__':
    main()

