import geo
import pandas as pd
import config
import io
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

result = []
comunas = geo.GEOLOCATION

for comuna in comunas:
    #name = comuna
    sub = len(geo.GEOLOCATION[comuna])
    print(sub)
    for i in range(0, sub):
        region = comuna
        commune = geo.GEOLOCATION[comuna][i]['name']
        lng = geo.GEOLOCATION[comuna][i]['lng']
        lat = geo.GEOLOCATION[comuna][i]['lat']
        activated = True
        
        data = {'region': region, 'commune': commune, 'lng': lng, 'lat': lat, 'activate': activated }
        result.append(data)

df = pd.DataFrame(result)           
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()      
 
df.head(0).to_sql(config.TOTTUS_GEOLOCATION['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.TOTTUS_GEOLOCATION['table_lz'], null="") # null values become ''
conn.commit()
connection.close()