from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from smtplib import SMTP
import config
from datetime import datetime
import pandas as pd
import time
from email import encoders
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import win32com.client as win32
    
def send_email(hour, date, company):
    file = "LEV "+company+" "+date+".xlsx"
    #configuracion mensaje
    recipients = [ 'pes4dill4@gmail.com','jordiiglesiasg@gmail.com', 'cristobal@gregario.cl']
    mensaje = MIMEMultipart("plain")
    mensaje["From"]="jiglesias@gregario.cl"
    mensaje["To"]= ", ".join(recipients)
    mensaje["Subject"] ="Informe Levantamiento digital "+company+" "+hour+":00"
    #texto
    texto = """
    Estimados,
        Se adjunta el informe de Levantamiento Digital.
        (mensaje automatico, no es necesario responder)
                """
    part1 = MIMEText(texto, "plain")
    #archivo adjunto
    adjunto = MIMEBase("application","octect-stream")
    adjunto.set_payload(open(file,"rb").read())
    encoders.encode_base64(adjunto)
    adjunto.add_header("content-Disposition",'attachment; filename='+file+'')
    mensaje.attach(adjunto)
    mensaje.attach(part1)
    #configuracion smtp
    smtp = SMTP("smtp.gmail.com")
    smtp.starttls()
    smtp.login("jiglesias@gregario.cl","86314188")
    smtp.sendmail("jiglesias@gregario.cl", recipients, mensaje.as_string())
    smtp.quit()
    
    return

def create_xls(date, company):
    file = "LEV "+company+" "+date+".xlsx"
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
                 , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
        
    connection = engine.connect()
    result = connection.execute(config.SQL['query']).fetchall()
       
    df = pd.DataFrame(result)
    df = df.sort_values(by=[0,1], ascending=True)
    header = ['Organización', 'Tienda', 'Direccion', 'Producto', 'Formato', 'Marca', 'Estado', 'Fecha' ]
    df.to_excel(file, index=False, header=header)
        
    connection.close()
    return 

def main():
    company = 'PF'
    c_hour = str(datetime.now().strftime("%H"))
    date = datetime.now().strftime("%d-%m-%Y %H")
    date = str(date)+'hrs'
    
    create_xls(date, company)
    time.sleep(5)
    #send_email(c_hour, date, company)
    
    

if __name__ == "__main__":
    main()


    