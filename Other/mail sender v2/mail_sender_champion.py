from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
#from pretty_html_table import build_table
from smtplib import SMTP
import config
from datetime import datetime
import pandas as pd
import log
import time
from email import encoders
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
from pretty_html_table import build_table
    
def send_email(hour, date, company, table_in):
    file = "LEV "+company+" "+date+".xlsx"
    #configuracion mensaje
    recipients = [ 'jiglesias@gregario.cl', 'JJerez@proa.cl', 'cnovoa@proa.cl', 'sdelajara@proa.cl', 'cristobal@gregario.cl', 'ignacio@gregario.cl']
    mensaje = MIMEMultipart("plain")
    mensaje["From"]="reportes@gregario.cl"
    mensaje["To"]= ", ".join(recipients)
    mensaje["Subject"] ="Informe Levantamiento digital "+company+" "+hour+":00"
    #texto
    texto = """<p>
   Estimados,<br>
       Los quiebres de stock de los productos Champion en la sucursal Jumbo Maipu son:<br>
       (mensaje automatico, no es necesario responder)</p>
                """
    txt_footer = """<p>Saludos,<br>Equipo Gregario.</p> """
    body_text = MIMEText(texto, "html")
    table = texto+table_in+'<br>'+txt_footer
    part1 = MIMEText(table, "html")
    #archivo adjunto
    #adjunto = MIMEBase("application","octect-stream")
    #adjunto.set_payload(open(file,"rb").read())
    #encoders.encode_base64(adjunto)
    #adjunto.add_header("content-Disposition",'attachment; filename='+file+'')
    #mensaje.attach(body_text)
    #mensaje.attach(adjunto)
    mensaje.attach(part1)
    #configuracion smtp
    smtp = SMTP("smtp.gmail.com")
    smtp.starttls()
    smtp.login("reportes@gregario.cl","gregario2021")
    smtp.sendmail("reportes@gregario.cl", recipients, mensaje.as_string())
    smtp.quit()
    
    return 1

def create_xls(date, company):
    file = "LEV "+company+" "+date+".xlsx"
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
                 , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
        
    connection = engine.connect()
    result = connection.execute(config.SQL_CHAMPION['query']).fetchall()
       
    df = pd.DataFrame(result)
    #df = df.sort_values(by=[0,1], ascending=True)
    #cabeceras tabla
    df.columns =['Retail', 'Producto', 'Formato', 'Marca', 'Estado', 'Revision'] 
    #excel
    #header = ['Retail', 'Producto', 'Formato', 'Marca', 'Estado', 'Revision' ]
    #df.to_excel(file, index=False, header=header)
        
    connection.close()
    return df

def main():
    company = 'Champion'
    c_hour = str(datetime.now().strftime("%H"))
    date = datetime.now().strftime("%d-%m-%Y %H")
    date = str(date)+'hrs'
    
    temp = create_xls(date, company)
    output = build_table(temp, 'blue_light')
    time.sleep(5)
    a = send_email(c_hour, date, company, output)
    
    if a == 1:
        log.insert_log('OK', 'Correo enviado')
    else:
        log.insert_log('ERROR', 'Correo no enviado')
    
    

if __name__ == "__main__":
    main()


    