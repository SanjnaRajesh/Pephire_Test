from ftplib import FTP
import pymysql
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient
from Autonomous_Job_Schedule import GetandUpdateJobDetails 

#Get the serer details and the file details from the DB

cnx = pymysql.connect(user='pephire@pepmysql', password='Nopassword4you',host='pepmysql.mysql.database.azure.com', database='pephire_auto')
cursor = cnx.cursor()

sql = "select * from pephire_auto.autonomous_job_file_master"
cursor.execute(sql)
auto_file_master = cursor.fetchall()
cursor.close()
cnx.close()
columns = [desc[0] for desc in cursor.description]

# Create a DataFrame from the fetched rows and column names
df_input_fileMaster = pd.DataFrame(auto_file_master, columns=columns)   

sql =''

for index, row in df_input_fileMaster.iterrows():
    storage_location = row['location']
    file_path = row['Path']
    file_name = row['filename']
    UnqFileID = row['id']
    createdTime = row['created_at']
    UpdatedTime = row['updated_at']
    DeletedTime = row['deleted_at']
    if storage_location == 'ftp':
        # FTP server details
        ftp_hostname = row['FTP_Hostname']
        ftp_username = row['FTP_Username']
        ftp_password = row['FTP_Password']
        
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
        GetandUpdateJobDetails(UnqFileID,file_name,df_FTP_file)
        
        
    elif storage_location == 'ADLS':
        #ADLS Details
        storage_acctKey = row['ADLS_StorageAccountKey']
        container_name = row['ADLS_ContainerName']
        storage_acctName = row['ADLS_StorageAccountName']
        
        # Create a credential object
        credential = DefaultAzureCredential()
        
        # Create a DataLakeFileClient
        file_client = DataLakeFileClient(account_url=f"https://{storage_acctName}.dfs.core.windows.net", file_system_name=file_name, file_path=file_path, credential=credential)
        
        df_adls = pd.read_excel(file_client.read_file())