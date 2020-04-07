from botocore.exceptions import ClientError
from tabulate import tabulate
import pandas as pd
import configparser 
import boto3
import json
from time import sleep

def createCluster():

    # Load DWH Params from a file
    config = configparser.ConfigParser() # creer le fichier de configuaration en memoire
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    ARN                    = config.get("IAM_ROLE", "ARN")
    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    DWH_DB                 = config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DB_PORT")

    

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })
                
    # Create clients for EC2, S3, IAM, and Redshift

    ec2 = boto3.resource('ec2', 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name="us-west-2")

    s3 = boto3.resource('s3', 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name="us-west-2")

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

    # Create roleArn
    print('\n')
    print('   --->> Check an Iam Role <<---   ') 
    roleArn=createRole(iam, DWH_IAM_ROLE_NAME)
    # Add roleArn in dwh.cfg #######
    config.set('IAM_ROLE','ARN', roleArn)
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

    # create cluster
    lunchCluster(redshift,roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,DWH_DB, DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD )



def createRole(iam, DWH_IAM_ROLE_NAME):
    ## STEP 1: IAM ROLE
    ## - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return(roleArn)


def lunchCluster(redshift,roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,DWH_DB, DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD ):
    ## STEP 2:  Redshift Cluster
    ## - Create a RedShift Cluster
    try:
        response = redshift.create_cluster(        
            # TODO: add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # TODO: add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,       
            
            # TODO: add parameter for role (to allow s3 access)
            IamRoles=[roleArn]
            
        )
    except Exception as e:
        print(e)

    ## 2.1 *Describe* the cluster to see its status
    ## - run this block several times until the cluster status becomes `Available`

    def prettyRedshiftProps(props):
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        props=pd.DataFrame(data=x, columns=["Key", "Value"])
        return(print(tabulate(props, headers='keys', tablefmt='rst', showindex=False)))

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    # time.sleep(20)

    while myClusterProps['ClusterStatus'] == 'creating' :
        print('Waiting for cluster to be ready ...')
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        sleep(30)
    if myClusterProps['ClusterStatus'] == 'available' :
        print("Cluster Created and running")




if __name__ == "__main__":
    print('###########################################################################')
    print('#                          First, create cluster                          #')
    print('###########################################################################')

    createCluster()

