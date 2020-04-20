from botocore.exceptions import ClientError
import pandas as pd
import configparser 
import boto3
import json
from time import sleep


def endCluster():
    path_cfg ='./redshift/'
    # Load DWH Params from a file
    config = configparser.ConfigParser() # creer le fichier de configuaration en memoire
    config.read_file(open(path_cfg + 'dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    # Create clients for EC2, S3, IAM, and Redshift
    iam = boto3.client('iam',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name="us-west-2"
                    )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                        )

    # delete cluster
    
    deleteCluster(redshift, DWH_CLUSTER_IDENTIFIER)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    while myClusterProps['ClusterStatus'] == 'deleting' :
        try :
            print('Waiting for cluster to be deleted ...')
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            sleep(30)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print("Cluster Doesn't exists")
                return
            else:
                print ("Unexpected error: %s" % e)

    deleteRole(iam, DWH_IAM_ROLE_NAME)


def deleteCluster(redshift, DWH_CLUSTER_IDENTIFIER):
        try:
            redshift.delete_cluster(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                SkipFinalClusterSnapshot=True
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print("Cluster Doesn't exists")
            else:
                print ("Unexpected error: %s" % e)

# Delete the iam role
def deleteRole(iam, DWH_IAM_ROLE_NAME):
    print("\n")
    print('IAM role is deleting...')
    try:
        iam.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        iam.delete_role(
            RoleName=DWH_IAM_ROLE_NAME
        )
    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print("IamRole Doesn't exists")
        else:
            print ("Unexpected error: %s" % e)
    return()

def main():
    endCluster()

if __name__ == "__main__":
    main()
