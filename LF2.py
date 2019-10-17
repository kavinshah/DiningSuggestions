from __future__ import print_function

import json
import boto3
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    sqs = boto3.client('sqs',aws_access_key_id="", aws_secret_access_key="",region_name="")
    queue_url = 'https://sqs.us-east-1.amazonaws.com/412391886323/DiningConcierge'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    print(response)
    
    if (response and 'Messages' in response):
        host = 'search-cloudproject-xid6zeahpe2kwsfpx6rltxamd4.us-east-1.es.amazonaws.com'
        
        region = 'us-east-1'
        service = 'es'
        #credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth('', '', region, service)
        
        es = Elasticsearch(
            hosts = [{'host': host, 'port': 443}],
            # http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('yelp-restaurants')
        
        for each_message in response['Messages']:
        
            message = each_message
            receipt_handle = message['ReceiptHandle']
            req_attributes = message['MessageAttributes']
            
            res_category = req_attributes['cuisine']['StringValue']
            # old query
            searchData = es.search(index="restaurants", body={
                                                            "query": {
                                                                "match": {
                                                                     "cuisine": res_category
                                                                }}})
          
            print (res_category)
            print (searchData)
           
            #print("Got %d Hits:" % searchData['hits']['total'])
            
            businessIds = []
            for hit in searchData['hits']['hits']:
                businessIds.append(hit['_source']['id'])
            
            # Call the dynemoDB
            resultData = getDynemoDbData(table, req_attributes, businessIds)
            print (resultData)
            print ('req_attributes----', req_attributes)
            
            # send the email
            #sendMailToUser(req_attributes, resultData)
            sendSMS(resultData, req_attributes['phone_number']['StringValue'])
            
            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
    else:
        print("No messages in queue")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Available messages digested')
    }

def getDynemoDbData(table, requestData, businessIds):
    
    if len(businessIds) <= 0:
        return 'We can not find any restaurant under this description, please try again.'
    
    textString = "Hello! Here are my " + requestData['cuisine']['StringValue'] + " restaurant suggestions for " + requestData['party']['StringValue'] +" people, for " + requestData['date']['StringValue'] + " at " + requestData['time']['StringValue'] + ". "
    count = 1
    
    for business in businessIds:
        responseData = table.query(KeyConditionExpression=Key('id').eq(business))
        #print(json.dumps(responseData.json(),indent=4))
        if (responseData and len(responseData['Items']) >= 1 and responseData['Items'][0]):
            responseData = responseData['Items'][0]
            display_address = ', '.join(responseData['location']['display_address'])
            
            textString = textString + " " + str(count) + "." + responseData['name'] + ", located at " + display_address + " "
            count += 1
    
    textString = textString + " Enjoy your meal!"
    return textString

def sendSMS(resultData, phone_number):
    print("in sendSMS")
    sns_client = boto3.client('sns',aws_access_key_id="", aws_secret_access_key="",region_name="")
    print("sending "+resultData+" to "+phone_number)
    response = sns_client.publish(
                                  PhoneNumber = "+1" + phone_number,
                                  Message=resultData
                                  )
    print("message sent")                              
    print(response)