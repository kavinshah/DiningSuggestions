import json
from datetime import datetime, timedelta
import time
import re
import boto3

def lambda_handler(event, context):
    intentName = event["currentIntent"]["name"]
    if(intentName == "DiningSuggestionsIntent"):
        return processDiningSuggestionsIntent(event)
    elif(intentName == "GreetingIntent"):
        return processGreetingIntent(event)
    elif(intentName == "ThankYouIntent"):
        return processThankYouIntent(event)

def validateSlot(event):
    location = event["currentIntent"]["slots"]["location"]
    party = event["currentIntent"]["slots"]["party"]
    date = event["currentIntent"]["slots"]["date"]
    time = event["currentIntent"]["slots"]["time"]
    cuisine = event["currentIntent"]["slots"]["cuisine"]
    phone_number = event["currentIntent"]["slots"]["phone_number"]
    Message = None
    
    if(location is not None and location.lower() not in ["new york", "manhattan", "new-york"]):
        location = None
        Message = "Sorry, valid locations are New York or Manhattan"
        slotToElicit = "location"
    elif (cuisine is not None and cuisine not in ["chinese","asian","oriental","italian","mexican","japanese","indian"]):
        cuisine = None
        Message = "Sorry, valid cuisines are chinese, asian, oriental, italian, mexican, japanese, indian"
        slotToElicit = "cuisine"
    elif (party is not None and (int(party) <= 0 or int(party) > 20)):
        party = None
        Message = "Sorry, we can only provide suggestions for a valid number of people upto 20"
        slotToElicit = "party"
    elif (date is not None and (datetime.strptime(date,"%Y-%m-%d")).date() < (datetime.now() + timedelta(hours=-4)).date()):
        date = None
        Message = "Sorry, we can only provide suggestions for valid dates in the future"
        slotToElicit = "date"
    elif (time is not None and (datetime.now() + timedelta(hours=-3,minutes=-30)) > datetime.combine(datetime.strptime(date,"%Y-%m-%d").date(), datetime.strptime(time,"%H:%M").time())):
        time = None
        Message = "Sorry, we can provide suggestions for requests that are atleast 30 minutes in the future"
        slotToElicit = "time"
    elif(phone_number is not None and len(re.sub("[^0-9]","",phone_number)) is not 10):
        Message = "Sorry, we didn't get that. Please provide a 10 digit phone number"
        phone_number = None
        slotToElicit = "phone_number"
        
    if(cuisine is not None):
        cuisine = cuisine.lower()
    
    if(Message is not None):
        return{
            "dialogAction": {
                "type": "ElicitSlot",
                "message" : {
                    "contentType" : "PlainText",
                    "content" : Message
                },
                "intentName" : "DiningSuggestionsIntent",
                "slotToElicit" : slotToElicit,
                "slots": {
                    'location': location,
                    'party': party,
                    'date': date,
                    'time': time,
                    'cuisine' :  cuisine,
                    'phone_number' : phone_number
                }
            }
        }
    else:
        return {
            "dialogAction": {
                "type": "Delegate",
                "slots": {
                    'location': location,
                    'party': party,
                    'date': date,
                    'time': time,
                    'cuisine' :  cuisine,
                    'phone_number' : phone_number
                }
            }
        }
    
def processDiningSuggestionsIntent(event):
    if(event["invocationSource"] == "DialogCodeHook"):
        return validateSlot(event)
    elif(event["invocationSource"] == "FulfillmentCodeHook" and event["currentIntent"]["confirmationStatus"] == "Confirmed"):
        #queue into SQS here
        sendToSQS(event["currentIntent"]["slots"]["location"], event["currentIntent"]["slots"]["party"], event["currentIntent"]["slots"]["date"], event["currentIntent"]["slots"]["time"], event["currentIntent"]["slots"]["cuisine"], event["currentIntent"]["slots"]["phone_number"])
        return{
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "You’re all set. Expect my suggestions shortly! Have a good day!"
                }
            }
        }
        
def processThankYouIntent(event):
    if(event["invocationSource"] == "FulfillmentCodeHook"):
        return{
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "You're Welcome!"
                }
            }
        }
        
def processGreetingIntent(event):
    if(event["invocationSource"] == "FulfillmentCodeHook"):
        return{
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "Hi there, how can I help?"
                }
            }
        }
        
def sendToSQS(location, party, date, time, cuisine, phone_number):
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/412391886323/DiningConcierge"
    messageAttributes = {
        'cuisine': {
            'DataType': 'String',
            'StringValue': cuisine
        },
        'location': {
            'DataType': 'String',
            'StringValue': location
        },
        'time': {
            'DataType': 'String',
            'StringValue': time
        },
        'date': {
            'DataType': 'String',
            'StringValue': date
        },
        'party': {
            'DataType': 'String',
            'StringValue': party
        },
        'phone_number': {
            'DataType': 'String',
            'StringValue': phone_number
        }
    }
    
    messageBody= ('Restaurant Suggestions')
    
    response = sqs.send_message(
        QueueUrl = queue_url,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
    )