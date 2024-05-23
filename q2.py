import boto3
import json
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def make_def(lambda_arn):
    definition = {
      "Comment": "Q2 State Machine",
      "StartAt": "Map",
      "States": {
        "Map": {
          "Type": "Map",
          "End": True,
          "MaxConcurrency": 10,
          "Iterator": {
            "StartAt": "Lambda Invoke",
            "States": {
              "Lambda Invoke": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "OutputPath": "$.Payload",
                "Parameters": {
                  "Payload.$": "$",
                  "FunctionName": lambda_arn
                },
                "Retry": [
                  {
                    "ErrorEquals": [
                      "Lambda.ServiceException",
                      "Lambda.AWSLambdaException",
                      "Lambda.SdkClientException",
                      "Lambda.TooManyRequestsException",
                      "States.TaskFailed",
                      "Lambda.Unknown"                      
                    ],
                    "IntervalSeconds": 2,
                    "MaxAttempts": 6,
                    "BackoffRate": 2
                  }
                ],
                "End": True
              }
            }
          }
        }
      }
    }
    return definition

if __name__ == '__main__':
    iam = boto3.client('iam')
    sfn = boto3.client('stepfunctions')
    aws_lambda = boto3.client('lambda')
    role = iam.get_role(RoleName='LabRole')
    lambda_function_name = 'q2'

    # Get Lambda Function ARN and Role ARN
    # Assumes Lambda function already exists
    lambda_arn = [f['FunctionArn']
                  for f in aws_lambda.list_functions()['Functions']
                  if f['FunctionName'] == lambda_function_name][0]
    
    # Throttle concurrent executions to 10
    response = aws_lambda.put_function_concurrency(
            FunctionName=lambda_function_name,
            ReservedConcurrentExecutions=10
        )

    sfn_function_name = 'q2'

    # Use Lambda ARN to create State Machine Definition
    sf_def = make_def(lambda_arn)

    # Create Step Function State Machine if doesn't already exist
    try:
        response = sfn.create_state_machine(
            name=sfn_function_name,
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn'],
            type='EXPRESS'
        )
    except sfn.exceptions.StateMachineAlreadyExists:
        response = sfn.list_state_machines()
        state_machine_arn = [sm['stateMachineArn'] 
                            for sm in response['stateMachines'] 
                            if sm['name'] == sfn_function_name][0]
        response = sfn.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn']
        )

    print('Began..')

    # generate all pages id
    base_url = 'http://books.toscrape.com/'
    url = base_url
    urls = []
    while True:
        urls.append(url)
        r = requests.get(url)
        html_soup = BeautifulSoup(r.text, 'html.parser')
        next = html_soup.select('li.next > a')
        if not next or not next[0].get('href'):
            break
        url = urljoin(url, next[0].get('href'))
    
    n = len(urls)
    #Spliting bataches
    print('total of pages:', n)
    quotient, remainder = divmod(n, 10)  # Calculate batches based on total URLs
    url_batches = []
    start_index = 0
    
    for i in range(10):
      if i < remainder:
        # First 'remainder' workers get 'quotient + 1' URLs each
        end_index = start_index + quotient + 1
      else:
        end_index = start_index + quotient # The rest get 'quotient' URLs each
      if end_index > start_index:  # Only add batches with URLs
        url_batches.append({'url': urls[start_index:end_index]})
      start_index = end_index
    
    print('batches set')
    
    #start execution 
    print('began execution...function has no return: check AWS for status and results')
    response = sfn.list_state_machines()
    state_machine_arn = [sm['stateMachineArn'] 
                        for sm in response['stateMachines'] 
                        if sm['name'] == sfn_function_name][0]
    # spread url batches across Lambda workers    
    response = sfn.start_sync_execution(
        stateMachineArn=state_machine_arn,
        name='url_10',
        input=json.dumps(url_batches)
    )