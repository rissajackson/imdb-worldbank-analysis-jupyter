import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 27)
}

staging_dataset = 'imdb_workflow_staging'
modeled_dataset = 'imdb_workflow_modeled'
orig_staging_dataset = 'imdb_staging'
movies_schema = '/home/jupyter/airflow/dags/movies_schema.json'
names_schema = '/home/jupyter/airflow/dags/names_schema.json'

bq_query_start = 'bq query --use_legacy_sql=false '

create_ratings = 'create or replace table ' + modeled_dataset + '''.Ratings as
                    select distinct *
                    from ''' + staging_dataset + '''.Ratings'''

create_movies = 'create or replace table ' + modeled_dataset + '''.Movies as 
                    select distinct *
                    from ''' + orig_staging_dataset + '''.Movies'''

create_actors = 'create or replace table ' + modeled_dataset + '''.Actors as 
                    select distinct * except(primary_profession, job, ordering, actors, production_company, place_of_death, reason_of_death, imdb_name_id, date_of_death), date_of_death as reason_of_death, generate_uuid() as actor_name_id
                    from ''' + orig_staging_dataset + '''.Names n inner join ''' + orig_staging_dataset + '''.Title_Principals t using (imdb_name_id) where t.category = "actor"'''

create_directors = 'create or replace table ' + modeled_dataset + '''.Directors as 
                    select distinct * except(primary_profession, job, ordering, actors, production_company, place_of_death, reason_of_death, imdb_name_id, date_of_death), date_of_death as reason_of_death, generate_uuid() as director_name_id
                    from ''' + orig_staging_dataset + '''.Names n inner join ''' + orig_staging_dataset + '''.Title_Principals t using (imdb_name_id) where t.category = "director"'''

create_writers = 'create or replace table ' + modeled_dataset + '''.Writers as 
                    select distinct * except(primary_profession, job, ordering, actors, production_company, place_of_death, reason_of_death, imdb_name_id, date_of_death), date_of_death as reason_of_death, generate_uuid() as writer_name_id
                    from ''' + orig_staging_dataset + '''.Names n inner join ''' + orig_staging_dataset + '''.Title_Principals t using (imdb_name_id) where t.category = "writer"'''

create_actors_movie= 'CREATE or replace table ' + modeled_dataset + '''.Actor_Movie AS
                    SELECT * EXCEPT(ordering, category, job)
                    FROM ''' + staging_dataset + '''.Title_Principals
                    WHERE category = "actor"'''

create_directors_movie= 'CREATE or replace table ' + modeled_dataset + '''.Director_Movie AS
                    SELECT * EXCEPT(ordering, category, job, characters)
                    FROM ''' + staging_dataset + '''.Title_Principals
                    WHERE category = "director"'''

create_writers_movie= 'CREATE or replace table ' + modeled_dataset + '''.Writer_Movie AS
                    SELECT * EXCEPT(ordering, category, job, characters)
                    FROM ''' + staging_dataset + '''.Title_Principals
                    WHERE category = "writer"'''

with models.DAG(
        'imdb_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_movies = BashOperator(
            task_id='load_movies',
            bash_command='bq --location=US load --skip_leading_rows=1 --source_format=CSV ' + staging_dataset + '.Movies "gs://nullbusters_data/movie_data/IMDb movies.csv" ' + movies_schema,
            trigger_rule='one_success')
    
    load_ratings = BashOperator(
            task_id='load_ratings',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Ratings \
                         "gs://nullbusters_data/movie_data/IMDb ratings.csv"',
            trigger_rule='one_success')
    
    load_title_principals = BashOperator(
            task_id='load_title_principals',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Title_Principals \
                         "gs://nullbusters_data/movie_data/IMDb title_principals.csv"', 
            trigger_rule='one_success')
    
    load_names = BashOperator(
            task_id='load_names',
            bash_command='bq --location=US load --skip_leading_rows=1 --allow_jagged_rows --source_format=CSV ' + staging_dataset + '.Names "gs://nullbusters_data/movie_data/IMDb names.csv" ' + names_schema, 
            trigger_rule='one_success')
    
    create_ratings = BashOperator(
            task_id='create_ratings',
            bash_command=bq_query_start + "'" + create_ratings + "'", 
            trigger_rule='one_success')
    
    create_movies = BashOperator(
            task_id='create_movies',
            bash_command=bq_query_start + "'" + create_movies + "'", 
            trigger_rule='one_success')
    
    create_actors = BashOperator(
            task_id='create_actors',
            bash_command=bq_query_start + "'" + create_actors + "'", 
            trigger_rule='one_success')
    
    create_directors = BashOperator(
            task_id='create_directors',
            bash_command=bq_query_start + "'" + create_directors + "'", 
            trigger_rule='one_success')
    
    create_writers = BashOperator(
            task_id='create_writers',
            bash_command=bq_query_start + "'" + create_writers + "'", 
            trigger_rule='one_success')
    
    create_actors_movie = BashOperator(
            task_id='create_actors_movie',
            bash_command=bq_query_start + "'" + create_actors_movie + "'", 
            trigger_rule='one_success')
    
    create_writers_movie = BashOperator(
            task_id='create_writers_movie',
            bash_command=bq_query_start + "'" + create_writers_movie + "'", 
            trigger_rule='one_success')
    
    create_directors_movie = BashOperator(
            task_id='create_directors_movie',
            bash_command=bq_query_start + "'" + create_directors_movie + "'", 
            trigger_rule='one_success')
    
    dataflow_writers = BashOperator(
            task_id='dataflow_writers',
            bash_command='python /home/jupyter/airflow/dags/Writers_beam_dataflow.py')
    
    dataflow_actors = BashOperator(
            task_id='dataflow_actors',
            bash_command='python /home/jupyter/airflow/dags/Actors_beam_dataflow.py')
    
    dataflow_directors = BashOperator(
            task_id='dataflow_directors',
            bash_command='python /home/jupyter/airflow/dags/Directors_beam_dataflow.py')

    dataflow_movies = BashOperator(
            task_id='dataflow_movies',
            bash_command='python /home/jupyter/airflow/dags/Movies_beam_dataflow.py')

    dataflow_new_movies = BashOperator(
            task_id='dataflow_new_movies',
            bash_command='python /home/jupyter/airflow/dags/New_Movies_beam_dataflow.py')
   
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
        
    create_staging >> create_modeled >> branch
    branch >> load_movies
    branch >> create_movies >> dataflow_movies >> dataflow_new_movies
    branch >> load_ratings >> create_ratings
    branch >> load_names
    branch >> load_title_principals >> create_actors >> create_directors >> create_writers >> create_actors_movie >> create_directors_movie >> create_writers_movie >> dataflow_directors >> dataflow_writers >> dataflow_actors