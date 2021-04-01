# -*- coding: utf-8 -*-
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
#from pretty_html_table import build_table
from smtplib import SMTP
import pandas as pd
from email import encoders
from pretty_html_table import build_table

def send_email(hour, date, company, table_in):
    file = "LEV "+company+" "+date+".xlsx"
    #configuracion mensaje
    recipients = [ 'pes4dill4@gmail.com', 'pato.negron.gue@gmail.com']
    mensaje = MIMEMultipart("plain")
    mensaje["From"]="jiglesias@gregario.cl"
    mensaje["To"]= ", ".join(recipients)
    mensaje["Subject"] ="LLegaron a "+company+" "+hour
    #texto
    texto = """
LLEGARON A SISTEMAX (mail automatico)
                """
    body_text = MIMEText(texto, "plain")
    table = table_in
    part1 = MIMEText(table, "html")
    #archivo adjunto
    #adjunto = MIMEBase("application","octect-stream")
    #adjunto.set_payload(open(file,"rb").read())
    #encoders.encode_base64(adjunto)
    #adjunto.add_header("content-Disposition",'attachment; filename='+file+'')
    mensaje.attach(body_text)
    #mensaje.attach(adjunto)
    mensaje.attach(part1)
    #configuracion smtp
    smtp = SMTP("smtp.gmail.com")
    smtp.starttls()
    smtp.login("jiglesias@gregario.cl","86314188")
    smtp.sendmail("jiglesias@gregario.cl", recipients, mensaje.as_string())
    smtp.quit()
    
    return 1


def main():
    result = []
    nombre = 'nada'
    url = 'https://www.sistemax.cl'
    # Se entrega el agente de browser o entrega error de acceso.
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    # Se hace un request a la página y queda todo almacenado en la variable.
    page_response = requests.get(url, headers=headers, timeout=30)
    
    # Se parsea el texto a html.
    soup = BeautifulSoup(page_response.content, "html.parser")
    content = soup.find_all("div", {"id": "content"})
    
    
    for i in content:
        items = i.find_all("div", {"class": "caption"})
        
        for a in items:
            #print(a)
            nombre = a.find('h4').text
            precio = a.find('span', {'class': 'price-new'}).text
    
            if "3060" in nombre or "3070" in nombre:
                info = {'nombre': nombre, 'precio': precio}
                result.append(info)
            else:
                continue
                          
        
    df = pd.DataFrame(result) 
    if len(df) > 0:      
        output = build_table(df, 'blue_light')
        company = 'Sistemax'
        c_hour = str(datetime.now().strftime("%H"))
        date = datetime.now().strftime("%d-%m-%Y %H")
        date = str(date)+'hrs'
 
        send_email(c_hour, date, company, output)
    else:
        pass
    
    return

if __name__ == "__main__":
    main()


