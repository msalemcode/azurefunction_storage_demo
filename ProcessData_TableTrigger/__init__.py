import logging
from time import sleep
import azure.functions as func
from azure.data.tables import TableClient
import os
import ssl
import csv
import json


ssl._create_default_https_context = ssl._create_unverified_context

def processRow(messageJSON):
    AZURE_CONNECTION_STRING = os.getenv('STORAGE_CONNECTION')
    print('Establish Connection with All Partners Table  .......................')
    existingPartners = TableClient.from_connection_string(conn_str=AZURE_CONNECTION_STRING,table_name="Partners")
    # Get list of all partners in the table
    partnersTotal= existingPartners.query_entities("")
    partnersCount=1;
    for item in partnersTotal:
        partnersCount=partnersCount+1;
    entity = {
                        "PartitionKey": str(partnersCount).zfill(8),
                        "RowKey": str(partnersCount).zfill(8),
                        "Alias": messageJSON[0],
                        "Name": messageJSON[1],
                        "AppId": messageJSON[2]
              }
    existingPartners.create_entity(entity=entity)
  


def main(req: func.HttpRequest, messageJSON) -> func.HttpResponse:

    message = json.loads(messageJSON)
    processRow(messageJSON)
    return func.HttpResponse(f"Table row: {messageJSON}")





