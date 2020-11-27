#This script when triggered, pushes the reference image from S3 into Spark for use in facial comparisson.

import json
import boto3
import botocore
import paramiko


def lambda_handler(event, context):
    
    k = paramiko.RSAKey.from_private_key_file("alpha.pem")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    #SSH details to log into Ubuntu instance on EC2
    client.connect(hostname="54.169.54.13", username="ubuntu", pkey=k, timeout=10)

    # Execute a command(cmd) after connecting/ssh to Ubuntu
    #stdin, stdout, stderr = client.exec_command("mkdir test")
    stdin, stdout, stderr = client.exec_command("cd bead-project-2020/reference_image;curl -o Reference_Image.jpg https://bead-project-target-upload-bucket.s3-ap-southeast-1.amazonaws.com/reference/Reference_Image.jpg")

    client.close()
    
    # TODO implement
    return {
        'statusCode': 200,
        'message':'Success!'}
    
