# Databricks notebook source
## Databse creation
def create_database():
  databases = ['xyz_bronze','xyz_silver','xyz_gold']
  status = ''
  execution_log = ''
  try:
    for database in databases:
      spark.sql(f'''create database IF NOT EXISTS {database}''')
    status = 'success'
    execution_log = f'execution of create_database -  succeeded '
  except Exception as execution_error:
    status = 'failed'
    execution_log = f'execution of create_database -  failed -  with error {str(execution_error)}'
  return status,execution_log
create_database = create_database()
create_database[0]  

# COMMAND ----------


def bronze_table_creation():
  try:
    for i in dbutils.fs.ls('/FileStore/CG_asgn'):
      file_path = f'/FileStore/CG_asgn/{i[1]}'
      dataset_name = i[1].replace('.csv','')
      spark.read.format('csv').options(header = True,sep = ',').load(file_path).createOrReplaceTempView(f'v_{dataset_name}')
      spark.sql(f'''CREATE TABLE IF NOT EXISTS xyz_bronze.{dataset_name} select * from v_{dataset_name}''')
    status = 'success'
    execution_log = f'Bronze table creation - succeeded ''' 
  except Exception as e:
    status = 'failed'
    execution_log = f'Bronze table creation - failed -  with error {str(execution_error)}'


# COMMAND ----------

def create_silver_tables():
  status = ''
  execution_log = ''
  try:
    spark.sql(f'''CREATE OR REPLACE TABLE xyz_silver.listing (
                  listing_id INT ,
                    property_type VARCHAR(255),
                    listing_title VARCHAR(255),
                    description STRING,
                    host_id INT,
                    price DECIMAL(10, 2),
                    availability_dates DATE,
                    rating INT,
                    minimum_nights INT,
                    neighbourhood varchar(255),
                    availability_365 INT) USING DELTA''')
## Neighborhoods Table
    spark.sql(f'''CREATE OR REPLACE TABLE  xyz_silver.neighbourhoods (
                  neighbourhood_name VARCHAR(255),
                  neighbourhood_group VARCHAR(255),
                  city VARCHAR(255),
                  region VARCHAR(255),
                  country VARCHAR(255)
                  )USING delta''')
## Reviews Table
    spark.sql(f'''CREATE OR REPLACE TABLE xyz_silver.reviews (
                  review_id INT ,
                  listing_id INT,
                  user_id INT,
                  review_text STRING,
                  rating DECIMAL(3, 2),
                  review_date DATE
                  ) USING delta''')
#Create the "ListingDates" table (optional)
    spark.sql(f'''CREATE OR REPLACE TABLE  xyz_silver.ListingDates (
                  ListingID INT,
                  AvailableDate DATE,
                  price string
              )USING delta ''')
    status = 'success'
    execution_log = f'execution of create_silver_tables -  succeeded '
  except Exception as execution_error:
    status = 'failed'
    execution_log = f'execution of create_silver_tables -  failed -  with error {str(execution_error)}'
  return status,execution_log

##### Gold table creation
def create_gold_tables():
  status = ''
  execution_log = ''
  try:
    ## Table Market Trends
    spark.sql(f'''CREATE OR REPLACE TABLE xyz_gold.market_trends (
                  trend_id INT ,
                  date DATE,
                  average_price DECIMAL(10, 2),
                  occupancy_rate DECIMAL(5, 2),
                  average_rating DECIMAL(3, 2)) USING DELTA
                  ''')
    
    ## Table property type statstics
    spark.sql('''CREATE TABLE xyz_gold.property_type_statistics (
                property_type VARCHAR(255) ,
                total_listings INT,
                average_price DECIMAL(10, 2),
                average_rating DECIMAL(3, 2)) USING DELTA
              ''')
    ## Table Neighborhood Statistics Table
    spark.sql('''CREATE TABLE xyz_gold.neighborhood_statistics (
                  neighborhood_id INT ,
                  total_listings INT,
                  average_price DECIMAL(10, 2),
                  average_rating DECIMAL(3, 2)) USING DELTA
              ''')
    spark.sql('''CREATE TABLE xyz_gold.user_engagement (
                  user_id INT,
                  total_reviews INT,
                  average_rating_given DECIMAL(3, 2),
                  favorite_neighborhood VARCHAR(255))USING DELTA
              ''')
    status = 'success'
    execution_log = f'execution of create_gold_tables -  succeeded '
  except Exception as execution_error:
    status = 'failed'
    execution_log = f'execution of create_gold_tables -  failed -  with error {str(execution_error)}'
  return status,execution_log


# COMMAND ----------

# ##inserting into listing table
def update_listing(tgt_tbl = 'xyz_silver.listing',src_tbl = 'v_listing',ref_tbl = 'v_list_det'):  
  status = ''
  execution_log = ''
  try:
    spark.sql(f'''insert into {tgt_tbl}
                  select 
                  l.id as listing_id,
                  l.room_type as property_type ,
                  l.name as listing_title,
                  ld.summary as description,
                  l.host_id as host_id,
                  l.price as price,
                  null as availability_dates,
                  null as rating,
                  l.minimum_nights as minimum_nights,
                  l.neighbourhood as neighbourhood,
                  l.availability_365 as availability_365
                  from {src_tbl} l inner join {ref_tbl} ld
                  on l.id = ld.id and l.host_id = ld.host_id
                  ''')
    status = 'success'
    execution_log = f'execution of update_listing -  succeeded -{tgt_tbl}  created '
  except Exception as execution_error:
    status = 'failed'
    execution_log = f'execution of update_listing -  failed -  with error {str(execution_error)}'
  return status,execution_log

##inserting into listing table
def update_neighbourhood(tgt_tbl = 'xyz_silver.neighbourhoods',src_tbl = 'v_ngh'):  
  status = ''
  execution_log = ''
  try:
    spark.sql(f'''insert into {tgt_tbl}
                 select 
                       neighbourhood as neighborhood_name ,           
                       neighbourhood_group as neighborhood_group,
                        null as city,
                        null as region,
                        null as country
                        from {src_tbl}
                  ''')
    status = 'success'
    execution_log = f'execution of update_neighbourhood -  succeeded - {tgt_tbl}  created '
  except Exception as execution_error:
    status = 'failed'
    execution_log = f'execution of update_neighbourhood -  failed -  with error {str(execution_error)}'
  return status,execution_log

  ###inserting into reviews table 
  def update_reviews(tgt_tbl = 'xyz_silver.reviews',src_tbl = 'v_reviews',ref_tbl ='v_rev_det'):
    status = ''
    execution_log = ''
    try:
      spark.sql(f'''INSERT into {tgt_tbl}
                  select  rd.id as review_id, 
                        r.listing_id as listing_id,
                        rd.reviewer_id as user_id,  
                        rd.comments as review_text,
                        null as rating,
                        r.date as review_date 
                  from {src_tbl} r 
                  inner join 
                  {ref_tbl} rd on r.listing_id = rd.listing_id and r.date = rd.date
                  where rd.listing_id is not null
                  and rd.date is not null
                ''')
      status = 'success'
      execution_log = f'execution of update_reviews -  succeeded - {tgt_tbl}  created '
    except Exception as execution_error:
      status = 'failed'
      execution_log = f'execution of update_reviews -  failed -  with error {str(execution_error)}'
    return status,execution_log

def update_ListingDates(tgt_tbl = 'xyz_silver.ListingDates',src_tbl = 'v_cal'):
  status = ''
  execution_log = ''
  try:
    spark.sql(f'''INSERT into {tgt_tbl}
                  select c.listing_id,c.date as AvailableDate,c.price from v_cal c
                  where c.available = 't'
                ''')
    status = 'success'
    execution_log = f'execution of update_ListingDates -  succeeded - {tgt_tbl}  created '
  except Exception as execution_error:
    status = 'failed'
    execution_log = f'execution of update_ListingDates -  failed -  with error {str(execution_error)}'
  return status,execution_log



# COMMAND ----------

#import org.apache.spark.sql.SparkSession
def orch():
  status = ""
  execution_log = ""
  try:
    create_database = create_database()
    if all([create_database[0] == 'success']):
      try:
            create_bronze_table = bronze_table_creation()
            create_silver_tables = create_silver_tables()
            listing= update_listing()
            neighbourhood = update_neighbourhood()
            reviews = update_reviews()
            listing_dates = update_ListingDates()
            if all([create_bronze_table[0] == create_silver_tables[0] == listing[0] == neighbourhood [0] == reviews[0] == listing_dates[0] == 'success']):
              status = 'success'
              execution_log = "bronze and silver table creation succeeded "
      except Exception as e:
        status = 'failed'
        execution_log = f"bronze and/or silver table creation failed, check log here {e} "
    else:
      status = 'failed'
      execution_log = f'Database creation failed'    
  except Exception as execution_error:
      status = 'failed'
      execution_log = f"orchestrator failed -- Kindly check the log here {execution_error}"

## initiating orchestrator
orch_exe = orch()
print(orch_exe)




