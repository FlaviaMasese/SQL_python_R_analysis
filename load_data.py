#%%
import os
from SqlDatabaseConnect import PsqlEngine
import pandas as pd
from constant import PASSWORD, DATABASE_USERNAME, PORT, HOST_NAME

current_dir = os.path.dirname(__file__)

#print(PASSWORD)
#print(DATABASE_USERNAME)
print(HOST_NAME)
#%%
#os.path.dirname(__file__)
data1_path = os.path.join(os.path.dirname(__file__), 'data/sect3_plantingw3.csv')
data2_path = os.path.join(current_dir, 'data/sect4c1_plantingw3.csv')
data3_path = os.path.join(current_dir, 'data/sect4c2_plantingw3.csv')
data4_path = os.path.join(current_dir, 'data/sect11b_harvestw3.csv')

#%%
sect3_plantingw3 = pd.read_csv(data1_path)
sect4c1_plantingw3 = pd.read_csv(data2_path)
sect4c2_plantingw3 = pd.read_csv(data3_path)
sect11b_harvestw3 = pd.read_csv(data4_path)

# %%
psql_int = PsqlEngine(username=DATABASE_USERNAME, password=PASSWORD, 
                      hostname=HOST_NAME,
                      port=PORT, 
                      database_name='lsms_data'
                      )

#%%
#%%
#psql_int.create_psql_database(database_name='lsms')


# psql_int.copy_data_to_psqldatabase(data=sect3_plantingw3, 
#                                    table_name='sect3_plantingw3', 
#                                    psql_copy_method=psql_int.psql_copy_method
#                                    )

# #%%
# psql_int.copy_data_to_psqldatabase(data=sect11b_harvestw3,
#                                    table_name='sect11b_harvestw3',
#                                    psql_copy_method=psql_int.psql_copy_method
#                                    )


#%%
table_names = ['sect3_plantingw3', 'sect4c1_plantingw3', 'sect4c2_plantingw3', 'sect11b_harvestw3']

table_data = [sect3_plantingw3, sect4c1_plantingw3, sect4c2_plantingw3, sect11b_harvestw3]


# %%
for name, data in  zip(table_names, table_data):
    psql_int.copy_data_to_psqldatabase(data=data, table_name=name,
                                       psql_copy_method=psql_int.psql_copy_method
                                    )
# %% try getting table
psql_int.get_table_as_dataframe(database_table_name='sect3_plantingw3')

# %% execute query to database

query = """SELECT state, lga, sector
            FROM sect3_plantingw3
            WHERE sector=1
            ORDER BY state;

        """
        
psql_int.fetch_sqlquery_as_dataframe(sqlquery_statement=query)

# %%
