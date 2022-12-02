# Librerías
import json
import boto3
import numpy as np
import pandas as pd
import requests
import io

# URLS para acceder a los archivos csv
urls = {
    'poblacion': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/poblacion.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T091614Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=a2ec20bc7fd1a1122d074659dfeda9c53ebad9a85648ba8bc54329768296bcd6',
    'fecundidad': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/fecundidad.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T023046Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=1fbcb5ca33f8261bf680a24ff8e99d59819d9ecaa4b1074eb64206ad8383b97e',
    'mortalidad': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/mortalidad.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T023706Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=fc432a0ab18585e474421968440127b65fa91e55528a37b83c1bceb2ffca1814',
    'migracion': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/migracion.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T030136Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=8d9638fb7198522f412393e347e2fff090f06553846084865fc1c2bce48bb8df',
    'hablantes': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/hablantes.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T030305Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=fa0f13372e827869eee4e4e234750cff60afb2aa18186054c4d1df42e14defaf',
    'discapacidad': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/discapacidad.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T030548Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=aa1715a4015ec95b77c2452339aca69eb15e03d1e1bd2e5457bb8568a9bbe129',
    'educacion': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/educacion.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T030725Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=4aa8e728b4be7b7bc80e6c706ce8418fe6c54bb071f1423074fe262a64b018ca',
    'economia': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/economia.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T030845Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=133423d712910836fa6d9c5ce6f8ce0a447d2c4df969acee6d00b212a0a69c17',
    'salud': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/salud.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T030949Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=a7e738fe11090e0fedee5ac0c10af37540345fc960fc76608eb03ba406f0f5b2',
    'situacion conyugal': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/situacion_conyugal.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T031224Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=95db41a2a68beae5751bc5488e4d1f3a2e0d7bff75053ba2ea15ec5f38fb523d',
    'religion': 'https://retoinegibucket.s3.us-west-2.amazonaws.com/religion.csv?X-Amz-Expires=86400&X-Amz-Date=20221201T031328Z&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARXWBOEHYDWOTTUYO%2F20221201%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=89072dd536cf2ed02bebe55eabb0a7b885cc25828a1bbf8c5f1b5f7a4cd08f6e'
}

def graficar(categoria, estado, informacion):
    '''
    Función que se encarga de procesar la información obtenida del bot para poder
    obtener la respuesta de la consulta para que posteriormente sea graficada en la 
    página web. 
    '''
    # Lectura de CSV
    url = urls[categoria]
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    response = response.content
    
    # Creación del dataframe
    df = pd.read_csv(io.StringIO(response.decode('utf-8')))
    df.drop(columns=['Unnamed: 0'], inplace = True)
    
    # Todos los estados
    if (estado == 'Todos'):
        x = list(df['ESTADO'])
        y = list(df[informacion])
        diccionario = {'x': x, 'y': y}
        jsonResultado = json.dumps(diccionario, indent=4)
        return jsonResultado
    # Un estado y una columnna
    if (estado != 'Todos' and informacion != 'Toda' and '-' not in informacion):
        df = df[df['ESTADO'] == estado]
        x = list(df['ESTADO'])
        y = list(df[informacion])
        diccionario = {'x': x, 'y': y}
        jsonResultado = json.dumps(diccionario, indent=4)
        return jsonResultado
    # Todos las columnas y un estado
    if (informacion == 'Toda'):
        df = df[df['ESTADO'] == estado]
        x = list(df.columns[1:])
        y = []
        for i in x:
            y.append(float(df[i]))
        diccionario = {'x': x, 'y': y}
        jsonResultado = json.dumps(diccionario, indent=4)
        return jsonResultado
    # Un estado y dos columnas (Femenino y Masculino)
    if ('-' in informacion):
        lista_informacion = informacion.split('-')
        df = df[df['ESTADO'] == estado]
        x = lista_informacion
        y = list(df[lista_informacion[0]]) + list(df[lista_informacion[1]])
        diccionario = {'x': x, 'y': y}
        jsonResultado = json.dumps(diccionario, indent=4)
        return jsonResultado
    else: 
        return df.columns

# Creating Session With Boto3.
session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='')

# Creating S3 Resource From the Session.
s3 = session.resource('s3')
txt_data = b'This is the content of the file uploaded from python boto3'
object = s3.Object('retoinegibucket', 'bot_response.txt')
result = object.put(Body=txt_data)

def lambda_handler(event, context):
    # Obtener slots
    intent_name = event['interpretations'][0]['intent']['name']
    slots = event['interpretations'][0]['intent']['slots']
    categoria = slots['Categoria']['value']['interpretedValue']
    estado = slots['Estado']['value']['interpretedValue']
    informacion = slots['Informacion']['value']['interpretedValue']
    
    # Consulta de información 
    resultado = graficar(categoria, estado, informacion)
    
    # Prueba pandas
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)
    message = str(resultado)
    
    # Esribir la respuesta final en el txt del s3
    txt_data = message
    object = s3.Object('retoinegibucket', 'bot_response.txt')
    result = object.put(Body=txt_data)

    response = {
       'sessionState' : {
            'dialogAction' : {
                'type' : 'Close'
            },
            'intent' : {
                'name' : intent_name,
                'state' : 'Fulfilled'
            }
       },
        'messages': [
             {
                'contentType' : 'PlainText',
                'content' : '¡Listo! Selecciona el botón de "ver gráfica" para desplegar los resultados.'
             }
        ]
    }

    return response