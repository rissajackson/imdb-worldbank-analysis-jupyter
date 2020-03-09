import datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatYearsFn(beam.DoFn):
  def process(self, element):
    directors = element
    name = directors.get('name')
    birth_name = directors.get('birth_name')
    height = directors.get('height')
    bio = directors.get('bio')
    birth_details = directors.get('birth_details')
    birth_year = directors.get('birth_year')
    place_of_birth = directors.get('place_of_birth')
    death_details = directors.get('death_details')
    death_year = directors.get('death_year')
    spouses = directors.get('spouses')
    divorces = directors.get('divorces')
    children = directors.get('children')
    known_for_titles = directors.get('known_for_titles')
    title = directors.get('imdb_title_id')
    category = directors.get('category')
    reason_of_death = directors.get('reason_of_death')
    director_name_id = directors.get('director_name_id')
    
    # turn floats into ints and checking for NULL
    
    if birth_year != None:
         new_birth = int(birth_year)
    else:
        new_birth = birth_year
        
    if death_year != None:
        new_death = int(death_year)
    else:
        new_death = death_year
    
    new_years = {"name":name,"birth_name":birth_name,"height":height,"bio":bio,"birth_details":birth_details,"birth_year":new_birth,"place_of_birth":place_of_birth,"death_details":death_details,"death_year":new_death,"spouses":spouses,"divorces":divorces,"children":children,"known_for_titles":known_for_titles,"imdb_title_id":title,"category":category,"reason_of_death":reason_of_death,"director_name_id":director_name_id}
    return [new_years]
    
def run():
    PROJECT_ID = 'swift-area-266618' # change to your project id
    BUCKET = 'gs://nullbusters_data' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'directors'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT name, birth_name, height, bio, birth_details, birth_year, place_of_birth, death_details, death_year, spouses, divorces, children, known_for_titles, imdb_title_id, director_name_id, category, reason_of_death FROM imdb_modeled.Directors WHERE birth_year IS NOT NULL AND death_year IS NOT NULL'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    query_results | 'Write log Input' >> WriteToText('input.txt')

    # apply ParDo to format directors birth year and death years to be ints  
    formatted_year_pcoll = query_results | 'Format Years' >> beam.ParDo(FormatYearsFn())

    # write PCollection to log file
    formatted_year_pcoll | 'Write log Output' >> WriteToText(DIR_PATH + 'output.txt')

    dataset_id = 'imdb_modeled'
    table_id = 'Directors_Beam_DF'
    schema_id = 'name:STRING,birth_name:STRING,height:FLOAT,bio:STRING,birth_details:STRING,birth_year:INTEGER,place_of_birth:STRING,death_details:STRING,death_year:INTEGER,spouses:INTEGER,divorces:INTEGER,children:STRING,known_for_titles:STRING,imdb_title_id:STRING,category:STRING,reason_of_death:STRING,director_name_id:STRING'


    
    # write PCollection to new BQ table
    formatted_year_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()