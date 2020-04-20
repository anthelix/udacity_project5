
## Legend
* task finish : Text
* task work : **Processing**
* hard time task : _Hard 
* reste a faire : code block

@daily
setup dwh.cfg and secret-remplate settings
conda env psyco
make redshift
make start



## Processing

* Create Docker
    * create users and database to simulate 
        * redshift (database "dest")
        * airflow adm metadabase de airflow
        * awsuser with grant privileges

    * create folder data with folders "log_data/" and "log_files/" ans data inside to simulate s3 (folder "data")

    *  Makefile
        * standart stuff
        * make tty (docker shell)
        * make psql ( docker psql)

* worflow
    * parametres
    * dag
    * tasks
        * Udacity_template
            * start_operator 
            * stage_events_to_redshift
            * stage_songs_to_redshift
            * load_songplays_table
            * load_user_dimension_table
            * load_song_dimension_table
            * load_artist_dimension_table
            * load_time_dimension_table
            * run_quality_checks
            * end_operator
        * Perso (brouillon udacity_dag, )
            * task start_operator
            * task print_hello()
                * `def print_hello()`
        * **create staging_tables for** check with the CLI when the cluster running
            * log_date 
            * song_data
        * StageToRedshiftOperator
            * stage_events_to_redshift: 
            * **stage_songs_to_redshift**: 
* Tomorrow: 
    * check with the cluster running, error on stage_redshift.py line 99
    * better to use iam_role redshift than awsuser in the copy stmt



```py

  
 
  * StageToRedshiftOperator
    * stage_events_to_redshift: 
    * stage_songs_to_redshift: 

  * load dimensions and fact tables from staging_tables: transformation
  * quality check
  * end_operator
```                 


This is the minimum settings required for the COPY query:
```py
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    COMPUPDATE OFF
    JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])
```

I found the solution. In case someone get the same problem

My code in the LoadDimensionOperator was :
```py
super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_statement = sql_statement,
        self.append_data=append_data
```

```py
def get_credentials():
    """
    get AWS keys
    """
    try:
        # parse file
        config = configparser.ConfigParser()
        config.read('dl.cfg')
        # set AWS variables
        os.environ['AWS_ACCESS_KEY_ID']     = config['AWS']['KEY']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']
    except Exception as e:
        print("Unexpected error: %s" % e)
```