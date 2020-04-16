from botocore.exceptions import ClientError
import pandas as pd
import configparser
from tabulate import tabulate
import boto3
import json
import psycopg2
from time import time
from sql_queries import create_table_queries, drop_table_queries


def connection():
    path_cfg ='/home/anthelix/Documents/projetGit/'
    # Load DWH Params from a file
    config = configparser.ConfigParser() # creer le fichier de configuaration en memoire
    config.read_file(open(path_cfg + 'dwh.cfg'))

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



    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name="us-west-2")



    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[4]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
    )
    except Exception as e:
        print(e)

    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
    print(conn_string)


def prettyRedshiftProps(props):
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    props=pd.DataFrame(data=x, columns=["Key", "Value"])
    return(print(tabulate(props, headers='keys', tablefmt='rst', showindex=False)))

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("1")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print("2")
        cur.execute(query)
        conn.commit()

def main():
    path_cfg ='/home/anthelix/Documents/projetGit/'
    print("           ")
    connection()
    config = configparser.ConfigParser()
    config.read(path_cfg + 'dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # print(conn)
    # drop_tables(cur, conn)
    # create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    print('###########################################################################')
    print('#                          Then, connect to the database                          #')
    print('###########################################################################')

    main()    
