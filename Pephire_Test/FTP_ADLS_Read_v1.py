from ftplib import FTP
import pymysql
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.blob import BlobServiceClient
# from Autonomous_Job_Schedule import GetandUpdateJobDetails 


def GetFileDetailsandLocation(UniqID):
#Get the serer details and the file details from the DB

    cnx = pymysql.connect(user='pephire@pepmysql', password='Nopassword4you',host='pepmysql.mysql.database.azure.com', database='pephire_auto')
    cursor = cnx.cursor()
    
    sql = """select * from pephire_auto.autonomous_job_file_master where id = ' """ +UniqID+ """ ' """
    cursor.execute(sql)
    auto_file_master = cursor.fetchall()
    cursor.close()
    cnx.close()
    columns = [desc[0] for desc in cursor.description]
    
    # Create a DataFrame from the fetched rows and column names
    df_input_fileMaster = pd.DataFrame(auto_file_master, columns=columns)   
    
    sql =''
    cursor.close()
    cnx.close()
    
    storage_location = df_input_fileMaster['location'][0]
    file_path = df_input_fileMaster['Path'][0]
    file_name = df_input_fileMaster['filename'][0]
    UnqFileID = df_input_fileMaster['id'][0]
    createdTime = df_input_fileMaster['created_at'][0]
    UpdatedTime = df_input_fileMaster['updated_at'][0]
    DeletedTime = df_input_fileMaster['deleted_at'][0]
    if storage_location == 'ftp':
        # FTP server details
        ftp_hostname = df_input_fileMaster['FTP_Hostname'][0]
        ftp_username = df_input_fileMaster['FTP_Username'][0]
        ftp_password = df_input_fileMaster['FTP_Password'][0]
        
        # Connect to the FTP server
        ftp = FTP(ftp_hostname)
        ftp.login(ftp_username, ftp_password)

        # Navigate to the directory where the file is located (if necessary)
        ftp.cwd(file_path)
        # Specify the file name to download
        filename = file_name
        
        with open(filename, 'wb') as file:
            ftp.retrbinary('RETR ' + filename, file.write)
            
        ftp.quit()
        
        df_FTP_file = pd.read_excel(filename)
        return df_FTP_file
        # GetandUpdateJobDetails(UnqFileID,file_name,df_FTP_file)
        
        
    elif storage_location == 'ADLS':
        #ADLS Details
        STORAGEACCOUNTKEY = df_input_fileMaster['ADLS_StorageAccountKey'][0]
        CONTAINERNAME = df_input_fileMaster['ADLS_ContainerName'][0]
        STORAGEACCOUNTNAME = df_input_fileMaster['ADLS_StorageAccountName'][0]
        BLOBNAME = df_input_fileMaster['FileName'][0]
        STORAGEACCOUNTURL = df_input_fileMaster['url'][0]
        
        STORAGEACCOUNTURL= 'https://senframeworkstoragenew.blob.core.windows.net'
        STORAGEACCOUNTKEY= 'jIszSeQZBaTULFYDQ3SVYcyAPxTEaDv85AYTHZJf6LhKvc3dpNvBX31DngPO2BsYHOampRNV8jAw+ASt31wJ2w=='
        CONTAINERNAME= 'testcontainer'
        BLOBNAME= 'Legendlist.csv'
        
        
        # df_adls = pd.read_csv('abfs://testcontainer@senframeworkstoragenew.dfs.core.windows.net/Legendlist.csv', storage_options = {'account_key' : 'jIszSeQZBaTULFYDQ3SVYcyAPxTEaDv85AYTHZJf6LhKvc3dpNvBX31DngPO2BsYHOampRNV8jAw+ASt31wJ2w=='})
        df_adls = pd.read_csv('abfs://'+CONTAINERNAME+'@'+STORAGEACCOUNTNAME+'.dfs.core.windows.net/'+file_path+'', storage_options = {'account_key' : STORAGEACCOUNTKEY})
        return df_adls