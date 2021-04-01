from sshtunnel import SSHTunnelForwarder
import numpy as np
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
import os

import config

today = date.today().strftime('%Y-%m-%d')

########################################################################################
## Conexiones a la base de datos.
########################################################################################

#Conexión al Tunnel SSH
server = SSHTunnelForwarder(
    (config.TUNNEL_SSH['host'], 22),
    ssh_username = config.TUNNEL_SSH['user'],
    ssh_password = config.TUNNEL_SSH['password'],
    remote_bind_address = (config.TUNNEL_SSH['rebind_address'], config.TUNNEL_SSH['rebind_address_port'])
    )
print('SSH Tunnel created')
server.start()


# Conexión a la base de datos Postgresql
local_port = str(server.local_bind_port)
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        config.DATABASE_CONFIG['user']
        , config.DATABASE_CONFIG['password']
        , config.DATABASE_CONFIG['host']
        , local_port
        , config.DATABASE_CONFIG['dbname']))
print('Engine create')

# Ejecución del SP que crea la lista de clientes para el día de hoy.
SP_execution = (pd.read_sql("SELECT gregario_labs.sp_fact_date_client_churn_trutro_pollo('"+today+"'::date)", engine).iat[0,0])
sql = config.SQL['query_trutro']

# Sigue con el resto del código, si el SP se ejecuta correctamente.
if SP_execution == 1 :
    
    ########################################################################################
    ## Creación archivo CSV
    ########################################################################################
    count = int(pd.read_sql(f"SELECT count(1) FROM ({sql}) a;", engine)['count'][0])
    print(f'sql has {count} rows')
    max_rows_allowed = 500000
    if count > max_rows_allowed:
        random_rows = np.min([max_rows_allowed,np.round(count*0.2)])
        print(f'estados_trutro: sample of {random_rows} rows')
        df = pd.read_sql(f"SELECT * FROM ({sql}) a  where random() < 0.01 limit {random_rows};", engine)
    else:
        df = pd.read_sql(f"{sql};", engine)
    df.to_csv(os.getcwd()+'\estados_trutro_'+today+'.csv',sep=';', index=False)
    print(f'estados_trutro saved as '+os.getcwd()+'\estados_trutro_'+today+'.csv')
    engine.dispose()
    print("Done and server stopped")
    server.stop()
    
    
    ########################################################################################
    ## Envío Mail
    ########################################################################################
    
    fromaddr = config.EMAIL_CONFIG['fromaddr']
    frompwd = config.EMAIL_CONFIG['frompwd']
    toaddr = config.EMAIL_CONFIG['toaddr']
    
    # instance of MIMEMultipart 
    msg = MIMEMultipart() 
    # storing the senders email address   
    msg['From'] = fromaddr 
    # storing the receivers email address  
    msg['To'] = toaddr 
    # storing the subject  
    msg['Subject'] = "Estados clientes: Trutro "+today
    # string to store the body of the mail 
    html = """\
    <html>
      <body>
        <p>Estimados,<br>
            Adjunto archivo con los estados del modelo de retención para Trutro.<br>
            <br>
            Saludos, Cristobal
        </p>
      </body>
    </html>
    """
    # attach the body with the msg instance 
    msg.attach(MIMEText(html, "html"))
    # open the file to be sent  
    filename = 'estados_trutro_'+today+'.csv'
    attachment = open(os.getcwd()+'\estados_trutro_'+today+'.csv', "rb") 
    # instance of MIMEBase and named as p 
    p = MIMEBase('application', 'octet-stream') 
    # To change the payload into encoded form 
    p.set_payload((attachment).read()) 
    # encode into base64 
    encoders.encode_base64(p) 
    p.add_header('Content-Disposition', "attachment; filename= %s" % filename) 
    # attach the instance 'p' to instance 'msg' 
    msg.attach(p) 
    # creates SMTP session 
    s = smtplib.SMTP('smtp.gmail.com', 587) 
    # start TLS for security 
    s.starttls() 
    # Authentication 
    s.login(fromaddr, frompwd)
    # Converts the Multipart msg into a string 
    text = msg.as_string() 
    # sending the mail 
    s.sendmail(fromaddr, toaddr.split(','), text) 
    # terminating the session 
    s.quit()
    print("Email sended")

print("End of process")