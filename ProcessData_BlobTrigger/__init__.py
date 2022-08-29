import logging
from time import sleep
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableClient
import os
import ssl
import csv

ssl._create_default_https_context = ssl._create_unverified_context

def processBlobFile(download_file):
    print('Start Scanning .......................')
    i=0
    with open(download_file,encoding="utf-8") as csvfile:
        AZURE_CONNECTION_STRING = os.getenv('STORAGE_CONNECTION')
        print('Establish Connection with All Partners Table  .......................')
        existingPartners = TableClient.from_connection_string(conn_str=AZURE_CONNECTION_STRING,table_name="Partners")
        # Get list of all partners in the table
        partnersTotal= existingPartners.query_entities("")
        partnersCount=1;
        for item in partnersTotal:
            partnersCount=partnersCount+1;
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        for row in readCSV:
            i=i+1
            print('Process raw --->'+str(i)+'--->' +row[0]+','+row[1]+','+row[2])
            entity = existingPartners.query_entities("AppId eq '"+row[2]+"'")
            print('Check if Entity Exists --->'+row[2])
            entityLength=0
            for e in entity: 
                entityLength=1
                break
            if(entityLength==0): 
                print('Store Partner Information')
                entity = {
                        "PartitionKey": str(partnersCount).zfill(8),
                        "RowKey": str(partnersCount).zfill(8),
                        "Alias": row[0],
                        "Name": row[1],
                        "AppId": row[2]
                    }
                existingPartners.create_entity(entity=entity)
                partnersCount=partnersCount+1


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    AZURE_CONNECTION_STRING = os.getenv('STORAGE_CONNECTION')
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container="datafile")
    print("Got Clients")
    data_file="/tmp/data.csv"

    if myblob.name=="datafile/data.csv":
        with open(data_file, "wb") as download_file:
            print("Start downlad")
            blobBytes = container_client.get_blob_client(myblob.name.replace('datafile/','')).download_blob().readall()
            download_file.write(blobBytes)
            download_file.close()
            
        if os.path.getsize(data_file)>0:
            processBlobFile(data_file)
    logging.info("Python blob trigger function completed \n")
