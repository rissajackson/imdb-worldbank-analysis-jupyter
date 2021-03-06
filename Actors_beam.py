import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatYearsFn(beam.DoFn):
  def process(self, element):
    actors = element
    name = actors.get('name')
    birth_name = actors.get('birth_name')
    height = actors.get('height')
    bio = actors.get('bio')
    birth_details = actors.get('birth_details')
    birth_year = actors.get('birth_year')
    place_of_birth = actors.get('place_of_birth')
    death_details = actors.get('death_details')
    death_year = actors.get('death_year')
    spouses = actors.get('spouses')
    divorces = actors.get('divorces')
    children = actors.get('children')
    known_for_titles = actors.get('known_for_titles')
    title = actors.get('imdb_title_id')
    category = actors.get('category')
    characters = actors.get('characters')
    reason_of_death = actors.get('reason_of_death')
    actor_name_id = actors.get('actor_name_id')
    
    # turn floats into ints and checking for NULL
    
    if birth_year != None:
         new_birth = int(birth_year)
    else:
        new_birth = birth_year
        
    if death_year != None:
        new_death = int(death_year)
    else:
        new_death = death_year
    
    new_years = {"name":name,"birth_name":birth_name,"height":height,"bio":bio,"birth_details":birth_details,"birth_year":new_birth,"place_of_birth":place_of_birth,"death_details":death_details,"death_year":new_death,"spouses":spouses,"divorces":divorces,"children":children,"known_for_titles":known_for_titles,"imdb_title_id":title,"category":category,"characters":characters,"reason_of_death":reason_of_death,"actor_name_id":actor_name_id}
    return [new_years]
    
def run():
     PROJECT_ID = 'swift-area-266618' # change to your project id

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT name, birth_name, height, bio, birth_details, birth_year, place_of_birth, death_details, death_year, spouses, divorces, children, known_for_titles, imdb_title_id, actor_name_id, category, characters, reason_of_death FROM imdb_modeled.Actors WHERE birth_year IS NOT NULL AND death_year IS NOT NULL limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
     query_results | 'Write log Input' >> WriteToText('input.txt')

     # apply ParDo to format actors birth year and death years to be ints  
     formatted_year_pcoll = query_results | 'Format Years' >> beam.ParDo(FormatYearsFn())

     # write PCollection to log file
     formatted_year_pcoll | 'Write log Output' >> WriteToText('output.txt')

     dataset_id = 'imdb_modeled'
     table_id = 'Actors_Beam'
     schema_id = 'name:STRING,birth_name:STRING,height:FLOAT,bio:STRING,birth_details:STRING,birth_year:INTEGER,place_of_birth:STRING,death_details:STRING,death_year:INTEGER,spouses:INTEGER,divorces:INTEGER,children:STRING,known_for_titles:STRING,imdb_title_id:STRING,category:STRING,characters:STRING,reason_of_death:STRING,actor_name_id:STRING'


    
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
    
    