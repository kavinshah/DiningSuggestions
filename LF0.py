import json
import boto3

def lambda_handler(event, context):
    
    client = boto3.client('lex-runtime')

    response = client.post_text(
        botName="DiningConcierge",
        botAlias="$LATEST",
        userId="frontend",
        inputText= event["userQuery"]
    )
    
    return {
        'statusCode': 200,
        'body': response["message"]
    }
