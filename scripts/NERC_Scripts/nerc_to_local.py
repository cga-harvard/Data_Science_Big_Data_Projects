#####################################################################
#                                                                   #
# This is a collection of functions for downloading files from NERC #
# to a local device.                                                #
#                                                                   #
# Author: Jack Hayes                                                #
# Edited by: Mukul Rawat                                            #
#                                                                   #
# *This was compiled on 10/02/2023 in Python 3.11.4*                #
#                                                                   #
#####################################################################

# import client library
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
import boto3
import os
import re
import pandas as pd

def get_creds(ec2_creds_path):
    """
    get_creds returns the access key and secret key from the EC2 credentials file found on Horizon
    
    Access this file by:
    - Logging in at https://stack.nerc.mghpcc.org/dashboard/auth/login/?next=/dashboard/
    - Select "Project" along the drop down banner up top
    - Navigate to "API Access"
    - Click the "Download OpenSTack RC File" drop down button on the right
    - Download "EC2 Credentials"

    Arguments:
        ec2_creds_path: STR - path to your EC2 credentials file

    Returns:
        ec2rc_access_key: STR - access key needed to interact with NERC
        ec2rc_secret_key: STR - secret key needed to interact with NERC
    """
    with open(ec2_creds_path, 'r') as file:
            script_content = file.read()

            pattern = re.compile(r'(EC2_ACCESS_KEY|EC2_SECRET_KEY)=(\S+)')
            matches = pattern.findall(script_content)

            creds = {}
            for variable, value in matches:
                creds[variable] = value

    ec2rc_access_key = creds["EC2_ACCESS_KEY"]
    ec2rc_secret_key = creds["EC2_SECRET_KEY"]

    return ec2rc_access_key, ec2rc_secret_key


def download_bucket(ec2rc_access_key, ec2rc_secret_key, local_output_path, bucket_name,
                    endpoint = 'https://stack.nerc.mghpcc.org:13808'):
                    
    """
    download_bucket downloads ALL files from a specified bucket in your NERC account

    Arguments:
        ec2rc_access_key: STR - access key from your EC2 Credentials (get_creds())
        ec2rc_secret_key: STR - secret key from your EC2 Credentials (get_creds())
        local_output_path: STR - desired local output directory/location path
        bucket_name: STR - name of the NERC bucket/container 
        endpoint: STR - common NERC endpoint

    Returns:
        None, files are downloaded locally
    """

    print("Reading bucket information...")
    s3_client = boto3.client('s3', aws_access_key_id=ec2rc_access_key, aws_secret_access_key=ec2rc_secret_key, 
                    endpoint_url=endpoint)
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    folders = []
    for obj in response['Contents']:
        if obj['Key'].endswith('/'):
            folders.append(obj['Key'])

    s3_resource = boto3.resource('s3',
            aws_access_key_id= ec2rc_access_key,
            aws_secret_access_key= ec2rc_secret_key,
            endpoint_url=endpoint,
        )
    bucket = s3_resource.Bucket(bucket_name)

    print("Downloading files...")
    for obj in bucket.objects.all():
        key = obj.key
        if key in folders:
            dir_name = key.split("/")[0]
            new_dir_path = os.path.join(local_output_path, dir_name)
            os.mkdir(new_dir_path)
            print(f"Sub directory {dir_name} created")
        else:
            bucket.download_file(key, os.path.join(local_output_path, key))

    return True


def download_single_file(ec2rc_access_key, ec2rc_secret_key, bucket_name, file, local_output_path, 
                         endpoint='https://stack.nerc.mghpcc.org:13808'):
    
    """
    download_single_file downloads a singular specified file in your NERC account

    Arguments:
        ec2rc_access_key: STR - access key from your EC2 Credentials (get_creds())
        ec2rc_secret_key: STR - secret key from your EC2 Credentials (get_creds())
        bucket_name: STR - name of the NERC bucket/container 
        file: STR - desired path to file from the bucket/container (ie. subfolder/text_file.txt)
        local_output_path: STR - desired local output directory/location path
        endpoint: STR - common NERC endpoint

    Returns:
        None, file is downloaded locally
    """
    s3 = boto3.resource('s3',
        aws_access_key_id=ec2rc_access_key,
        aws_secret_access_key=ec2rc_secret_key,
        endpoint_url=endpoint
    )

    print(f"Downloading file {file} from bucket {bucket_name} to {local_output_path}...")

    try:
        # Ensure the local_output_path directory exists, or create it
        os.makedirs(local_output_path, exist_ok=True)
        
        # Download the file to the specified local path
        local_file_path = os.path.join(local_output_path, os.path.basename(file))
        s3.Bucket(bucket_name).download_file(file, local_file_path)
        
        print(f"Downloaded {file} to {local_file_path}")
        return True
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        return False


def read_nerc_csv(ec2rc_access_key, ec2rc_secret_key, bucket_name, csv_file_path, 
                         endpoint='https://stack.nerc.mghpcc.org:13808'):
    
    """
    read_nerc_csv reads a singular specified file in your NERC account into your environment
    
    THIS IS MORE OF A FRAMEWORK OF A FUNCTION AND DEMONSTRATION OF PROCESS
    Note that you will have to manually edit the pd.read_csv() method to specify
    desired encoding, compression type, delimeters, etc.

    Arguments:
        ec2rc_access_key: STR - access key from your EC2 Credentials (get_creds())
        ec2rc_secret_key: STR - secret key from your EC2 Credentials (get_creds())
        bucket_name: STR - name of the NERC bucket/container 
        csv_file_path: STR - desired path to file from the bucket/container (ie. subfolder/csv_file.csv)
        endpoint: STR - common NERC endpoint

    Returns:
        Pandas dataframe of specified csv file
    """

    s3_resource = boto3.resource('s3',
                             aws_access_key_id= ec2rc_access_key,
                             aws_secret_access_key= ec2rc_secret_key,
                             endpoint_url=endpoint)
                             
    bucket = s3_resource.Bucket(bucket_name)
    obj = bucket.Object(csv_file_path)
    body = obj.get()['Body']
    df = pd.read_csv(body)

    return df


def download_folder(ec2rc_access_key, ec2rc_secret_key, bucket_name, folder_path, local_output_path, 
                   endpoint='https://stack.nerc.mghpcc.org:13808'):
    """
    download_folder downloads all files within a specified folder in your NERC account

    Arguments:
        ec2rc_access_key: STR - access key from your EC2 Credentials (get_creds())
        ec2rc_secret_key: STR - secret key from your EC2 Credentials (get_creds())
        bucket_name: STR - name of the NERC bucket/container 
        folder_path: STR - desired path to the folder in the bucket/container
        local_output_path: STR - desired local output directory/location path
        endpoint: STR - common NERC endpoint

    Returns:
        None, all files within the folder are downloaded locally
    """
    s3 = boto3.resource('s3',
        aws_access_key_id=ec2rc_access_key,
        aws_secret_access_key=ec2rc_secret_key,
        endpoint_url=endpoint
    )

    print(f"Downloading files from folder {folder_path} in bucket {bucket_name} to {local_output_path}...")

    try:
        # Ensure the local_output_path directory exists, or create it
        os.makedirs(local_output_path, exist_ok=True)
        
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=folder_path):
            if obj.key != folder_path:  # Exclude the folder itself
                # Construct the local file path by using the entire object key
                local_file_path = os.path.join(local_output_path, os.path.basename(obj.key))
                while os.path.exists(local_file_path):
                    local_file_path = os.path.join(local_output_path, 'duplicate_' + os.path.basename(obj.key))
                bucket.download_file(obj.key, local_file_path)
                # print(f"Downloaded {obj.key} to {local_file_path}")
        
        print(f"All files from folder {folder_path} downloaded to {local_output_path}")
        return True
    except Exception as e:
        print(f"Error downloading folder: {str(e)}")
        return False


