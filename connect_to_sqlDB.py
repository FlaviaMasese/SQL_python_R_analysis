#%%
import psycopg2


# %%
with psycopg2.connect(host='localhost', user="postgres",
                      password=4411, dbname='postgres', 
                      port=5432) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM customers LIMIT 5")
        records = cur.fetchall()
        
records


# %% SQLAlchemy
from sqlalchemy import create_engine
import pandas as pd

# %%  define connection string
cnxn_string = ("postgresql+psycopg2://{username}:{pswd}"
               "@{host}:{port}/{database}")

print(cnxn_string)


#%% fill in parameters to create database engine
engine = create_engine(cnxn_string.format(
                        username="postgres", pswd=4411, host="localhost",
                        port=5432, database="postgres"
                    ))


#%% excute commands to the database
engine.execute("SELECT * FROM customers LIMIT 2;").fetchall()



# %% use engine to read data into pandas
customers_data = pd.read_sql_table('customers', engine)
customers_data

# %% define a query
query = """
SELECT city,
        COUNT(1) AS number_of_customers,
        COUNT(NULLIF(gender, 'M')) AS female,
        COUNT(NULLIF(gender, 'F')) AS male
FROM customers
WHERE city IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10

"""

# %% read qury into pandas
top_cities_data = pd.read_sql_query(query, engine)

# %% plot data
ax = top_cities_data.plot.bar('city', y=['female', 'male'],
                              title='Number of customers by Gender and City')


#%% write to database
top_cities_data.to_sql(name='top_cities_data', con=engine,
                       index=False, if_exists='replace')



# %% USING COPY of psql in python
import csv
from io import StringIO

#%%
def psql_insert_copy(table, conn, keys, data_iter):
    # get a DNAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        
        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name
            
        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                    table_name, columns
                )
        cur.copy_expert(sql=sql, file=s_buf)
        
        








# %%
