import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class AddCurrencyFn(beam.DoFn):
  def process(self, element):
    movies = element
    imdb_title_id = movies.get('imdb_title_id')
    title = movies.get('title')
    original_title = movies.get('original_title')
    year = movies.get('year')
    genre = movies.get('genre')
    duration = movies.get('duration')
    country = movies.get('country')
    language = movies.get('language')
    director = movies.get('director')
    writer = movies.get('writer')
    production_company = movies.get('production_company')
    actors = movies.get('actors')
    description = movies.get('description')
    avg_votes = movies.get('avg_votes')
    votes = movies.get('votes')
    budget_currency = movies.get('budget_currency')
    budget = movies.get('budget')
    usa_gross_income = movies.get('usa_gross_income')
    worlwide_gross_income_currency = movies.get('worlwide_gross_income_currency')
    worlwide_gross_income = movies.get('worlwide_gross_income')
    metascore = movies.get('metascore')
    reviews_from_users = movies.get('reviews_from_users')
    reviews_from_critics = movies.get('reviews_from_critics')
    
    # splitting country into two columns
    if (country != None): # check if NULL
        country_list = country.split(", ")
        for i, country_type in enumerate(country_list):
            if "USA" == country_type:
                country_list[i] = "United States"
            if "UK" == country_type:
                country_list[i] = "United Kingdom"
        main_country = country_list.pop(0)
        other_countries = ", ".join(country_list) or None
    else:
        main_country = country
        other_countries = None
    
    # removing currency symbol from usa_gross_income

    """
    if (usa_gross_income != None): # check if NULL
        money = str(usa_gross_income)
        usa_currency, income = money.split(" ")
        new_usa_income = int(income) # new income as integer
    else:
        new_usa_income = usa_gross_income
        
    # splitting worlwide_gross_income into currency and income
    """
    """
    if (worlwide_gross_income != None): # check if NULL
        money = str(worlwide_gross_income)
        data = money.split(" ")
        worl_currency = data[0]
        new_worl_income = data[1]
    else:
        new_worl_income = usa_gross_income
        worl_currency = None
        """
    
    new_results = {"imdb_title_id":imdb_title_id,"title":title,"original_title":original_title,"year":year,"genre":genre,"duration":duration,"country":main_country, "other_countries": other_countries, "language":language, "director":director, "writer":writer, "production_company":production_company,"actors":actors,"description":description,"avg_votes":avg_votes,"votes":votes,"budget_currency":budget_currency,"budget":budget,"usa_gross_income":usa_gross_income,"worlwide_gross_income_currency":worlwide_gross_income_currency,"worlwide_gross_income":worlwide_gross_income,"metascore":metascore,"reviews_from_users":reviews_from_users,"reviews_from_critics":reviews_from_critics}
    return [new_results]


def run():
     PROJECT_ID = 'swift-area-266618' # change to your project id

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT imdb_title_id, title, original_title, year, genre, duration, country, language, director, writer, production_company, actors, description, avg_votes, votes, budget_currency, budget, usa_gross_income, worlwide_gross_income_currency, worlwide_gross_income, metascore, reviews_from_users, reviews_from_critics FROM imdb_modeled.Movies_Beam_DF WHERE country IS NOT NULL limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
     query_results | 'Write log Input' >> WriteToText('input.txt')

     # apply ParDo to format actors birth year and death years to be ints  
     formatted_year_pcoll = query_results | 'Format Years' >> beam.ParDo(AddCurrencyFn())

     # write PCollection to log file
     formatted_year_pcoll | 'Write log Output' >> WriteToText('output.txt')

     dataset_id = 'imdb_modeled'
     table_id = 'New_Movies_Beam'
     schema_id = 'imdb_title_id:String, title:STRING, original_title:STRING, year:INTEGER, genre:STRING, duration:INTEGER, country:STRING, other_countries:STRING, language:STRING, director:STRING, writer:STRING, production_company:STRING, actors:STRING, description:STRING, avg_votes:FLOAT, votes:INTEGER, budget_currency:STRING, budget:INTEGER, usa_gross_income:INTEGER, worlwide_gross_income_currency:STRING, worlwide_gross_income:INTEGER, metascore:FLOAT, reviews_from_users:FLOAT, reviews_from_critics:FLOAT'
    
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