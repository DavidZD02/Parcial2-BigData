import urllib.request
import datetime
import boto3

def descargar(url,nombre):
    bucket='noticias-parcial99'
    response = urllib.request.urlopen(url)
    contenido = response.read().decode('utf-8')
    fecha = datetime.date.today().strftime('%Y-%m-%d')
    s3 = boto3.resource('s3')
    s3.Object(bucket, 'headlines/raw/{}{}.html'.format(
        nombre, fecha)).put(Body=contenido)
        
descargar('https://www.eltiempo.com', 'elTiempo-')
descargar('https://www.elespectador.com',  'elEspectador-')