from distutils.command.upload import upload
import numpy as np
import pandas as pd
import os
import xlrd
from pprint import pprint
from google.cloud import storage

def read_from_file(file_name, sheet_name):
    path = os.getcwd() + os.sep + file_name 
    wb = xlrd.open_workbook(path, encoding_override="cp1252", formatting_info=True)
    sheet = wb.sheet_by_name(sheet_name) 
    skip_rows = 4
    data = []
    for row in range(sheet.nrows - skip_rows):
        d_row = []
        for col in range(sheet.ncols):
            d_row.append(sheet.cell_value(row+skip_rows, col))
        data.append(d_row)

    np_array = np.array(data)
    df = pd.DataFrame(np_array)
    return df

def rename_columns(df, period):
    # https://sparkbyexamples.com/pandas/pandas-rename-column-by-index/
    # df.rename(columns={df.columns[2]: 'Courses_Duration'},inplace=True)
    df_new = df.rename(columns={df.columns[0]: 'BANCO', df.columns[1]: 'CARTERA', df.columns[2]:'% DE PARTICIP.', df.columns[3]:'LUGAR RANKING'})
    df_new['Periodo'] = period
    return df_new

def extract_entity(df, start, nrows):
    df_bm = df.iloc[start+1:start+nrows+1,1:] # 3:17,1:
    df_new = rename_columns(df_bm.iloc[:,0:4], df.iloc[0,2])
    df_new = df_new.append( rename_columns(df_bm.iloc[:,[0,4,5,6]], df.iloc[0,5]) )
    df_new = df_new.append( rename_columns(df_bm.iloc[:,[0,7,8,9]], df.iloc[0,8]) )
    df_new = df_new.append( rename_columns(df_bm.iloc[:,[0,10,11,12]], df.iloc[0,11]) )
    df_new['Tipo Entidad'] = df.iloc[start,1] # 2,1
    df_new.reset_index(drop=True, inplace=True)
    return df_new


def etl_depositos(file_name):
    df = read_from_file('RankingDepositos.XLS', '9648')
    
    df_result = extract_entity(df, 2,14)   
    df_py = extract_entity(df, 19, 2)
    df_dp = extract_entity(df, 24, 1)
    df_viv = extract_entity(df, 28, 3)
    df_coo = extract_entity(df, 34, 37)
    df_ifr = extract_entity(df, 74, 4)
    
    df_result = df_result.append(df_py)
    df_result = df_result.append(df_dp)
    df_result = df_result.append(df_viv)
    df_result = df_result.append(df_coo)
    df_result = df_result.append(df_ifr)
    
    df_result.reset_index(drop=True, inplace=True)
    df_result.to_csv(file_name, sep='|', index=False)
    
    return df_result

def etl_cartera(file_name):
    
    df = read_from_file('RankingColocaciones.XLS', '9649')

    df_result = extract_entity(df, 2,14)   
    df_py = extract_entity(df, 19, 2)
    df_dp = extract_entity(df, 24, 1)
    df_viv = extract_entity(df, 28, 3)
    df_coo = extract_entity(df, 34, 37)
    df_ifr = extract_entity(df, 74, 9)
    
    df_result = df_result.append(df_py)
    df_result = df_result.append(df_dp)
    df_result = df_result.append(df_viv)
    df_result = df_result.append(df_coo)
    df_result = df_result.append(df_ifr)
    
    df_result.reset_index(drop=True, inplace=True)
    df_result.to_csv(file_name, sep='|', index=False)
    
    return df_result

def procesar(file_path):
    print('Ruta de Procesamiento:', os.getcwd())
    os.chdir(file_path)

    etl_depositos(file_name='depositos.csv')
    etl_cartera(file_name='colocaciones.csv')

#cargar archivo al bucket
def upload_to_bucket(blob_name, file_path, bucket_name):
    #instanciar la clave de conexcion a GCP
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'../[GCP CREDENTIALS].json' #r'../inner-strategy-351517-ac74e7e8bbe6.json'
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    return blob

def upload():
    #nombre del bucket
    bucket_name = 'ultima-clase-teorica'
    os.chdir('../data')
    print(os.getcwd())
    response = upload_to_bucket('datalake/WORKLOAD/banca/colocaciones.csv', 'colocaciones.csv', bucket_name)
    response = upload_to_bucket('datalake/WORKLOAD/banca/depositos.csv', 'depositos.csv', bucket_name)

if __name__ == '__main__':
    print('Tranforming Excel Data...')
    procesar('../data')

    print('Uploading to Bucket.....')
    upload()

    print('Finalizado')