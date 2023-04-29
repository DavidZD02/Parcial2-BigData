import boto3
from bs4 import BeautifulSoup
from datetime import date

bucket = 'noticias-parcial99'
fecha = date.today()

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


def elTiempo(file):
    contenido = s3.get_object(Bucket=bucket, Key=file)["Body"].read()
    soup = BeautifulSoup(contenido, 'html.parser')

    texto = 'titulo,categoria,link\n'
    enlaces_vistos = []
    for articles in soup.find_all('article'):
        for links in articles.find_all('a', class_='title page-link'):
            try:
                link = links["href"]
                if link not in enlaces_vistos:
                    enlaces_vistos.append(link)
                    categoria = link.split('/')[1]
                    titulo = links.text.strip()
                    texto += f'"{titulo}","{categoria}","{link}"\n'
            except KeyError:
                pass

    url = "headlines/final/periodico=elTiempo/year=" + \
        str(fecha.year)+"/month="+str(fecha.strftime('%m'))+"/elTiempo"
    s3_resource.Object(bucket, url+'{}.csv'.format(
      fecha.strftime('%Y-%m-%d'))).put(Body=texto)


def elEspectador(file):
    contenido = s3.get_object(Bucket=bucket, Key=file)["Body"].read()
    soup = BeautifulSoup(contenido, 'html.parser')
    texto = 'titulo,categoria,link\n'

    for articles in soup.find_all('h2', class_='Card-Title Title Title'):
        for links in articles.find_all('a'):
            try:
                link = '"'+links["href"]+'"'
                categoria = '"'+link.split('/')[1]+'"'
                titulo = '"'+(links.text)+'"'
                texto += f'{titulo},{categoria},{link}\n'
            except KeyError:
                pass

    url = "headlines/final/periodico=elEspectador/year="+str(
      fecha.year)+"/month="+str(fecha.strftime('%m'))+"/elEspectador"
    s3_resource.Object(bucket, url+'{}.csv'.format(
      fecha.strftime('%Y-%m-%d'))).put(Body=texto)


elTiempo("headlines/raw/elTiempo-" + str(fecha.year) + "-" + str(
  fecha.strftime('%m')) + "-" + str(fecha.strftime('%d')) + ".html")
elEspectador("headlines/raw/elEspectador-" + str(fecha.year) + "-" + str(
  fecha.strftime('%m')) + "-" + str(fecha.strftime('%d')) + ".html")
