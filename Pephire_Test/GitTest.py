import pandas as pd
import pymysql
from datetime import datetime
from datetime import date,timedelta
# from FTP_ADLS_Read_v1 import GetFileDetailsandLocation
from GetJDSkills_WorkingWithoutDB import getMandatorykSkill
import win32
import win32com.client
from JDFitment_FANewFunc_testing import JDFitment
from dateutil.relativedelta import relativedelta
from Config import user_val, password_val,host_val,database_val ,logFlag,pephire_db_trans,currDir,lang,pephire,pephire_trans
from Lib import PepLog
from Lib_v1 import db_read,db_write,logger

#Get File Status from Autonomous file run status
try:

    cnx = pymysql.connect(user='pephire@pepmysql', password='Nopassword4you',host='pepmysql.mysql.database.azure.com', database='pephire_auto')

    cursor = cnx.cursor()

   

    PepLog(logFlag,'Connection created to read from run status table')

    #Get the run status of the files.

    sql = "select * from pephire_auto.autonomous_file_run_status"

    PepLog(logFlag,sql)

 

    cursor.execute(sql)

    filerunstatus = cursor.fetchall()

    columns_filerunstatus = [desc[0] for desc in cursor.description]

    sql = ''

    # Create a DataFrame from the fetched rows and column names

    df_file_runstatus = pd.DataFrame(filerunstatus, columns=columns_filerunstatus)

    

    PepLog(logFlag,'Connection created to read from autonomous job')

 

    #Read Autonomous Jobs table

    sql = "select * from pephire_auto.autonomous_job"

    PepLog(logFlag,sql)

 

    cursor.execute(sql)

    autonomousjobstable = cursor.fetchall()

    columns_autonomousjobstable = [desc[0] for desc in cursor.description]

   

    # Create a DataFrame from the fetched rows and column names

    df_autonomousjobs = pd.DataFrame(autonomousjobstable, columns=columns_autonomousjobstable)

    

    sql = ''

   

    PepLog(logFlag,'Connection created to read from autonomous job schedule')

 

    #Extract unique file id and scheduling details

   

    sql = "select * from pephire_auto.autonomous_job_schedule"

    PepLog(logFlag,sql)

    cursor.execute(sql)

    PepLog(logFlag,sql)

    autonomousjobs = cursor.fetchall()

    columns_autonomousjobs = [desc[0] for desc in cursor.description]

   

    

    # Create a DataFrame from the fetched rows and column names

    df_autonomousjob_schedule = pd.DataFrame(autonomousjobs, columns=columns_autonomousjobs)

    sql = ''

    cursor.close()

    cnx.close()

except Exception as e:

    cursor.close()

    cnx.close()

    PepLog(logFlag,e)

def GetLastRunTime(UniqueFileId):

    #This function returns the next run time for the given unque file id

    try:

        PepLog(logFlag,'Entered GetLastRunTime function')

 

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

        sql = """select NextRun from pephire_auto.autonomous_file_run_status where UniqueFileID = '"""+UniqueFileId+"""' and AsOfDate in (select max(AsOfDate) from pephire_auto.autonomous_file_run_status where UniqueFileID = '"""+UniqueFileId+"""')"""

        PepLog(logFlag,sql)

 

        cursor.execute(sql)

       

        lastrun = cursor.fetchall()

        columns_lastrun = [desc[0] for desc in cursor.description]

   

        # Create a DataFrame from the fetched rows and column names

        df_autonomouslastrun = pd.DataFrame(lastrun, columns=columns_lastrun)

        sql = ''

        cursor.close()

        cnx.close()

        return df_autonomouslastrun['NextRun'][0]

    except Exception as e :

        PepLog(logFlag,'Entered Exit block of GetLastRunTime function')

        cursor.close()

        cnx.close()

        PepLog(logFlag,e)

def CheckinTimeTravelTable(FileId):

    #This function checks if the added file is a new one or existing one. If the file Id is present

    # in the time travel table, the file is an existing one

    try:

        PepLog(logFlag,'Entered CheckinTimeTravelTable function')

 

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

   

        sql = """select * from pephire_auto.autonomous_job_time_travel where UniqueFileID = '"""+FileId+"""' """

        PepLog(logFlag,sql)

 

        cursor.execute(sql)

        timetravel = cursor.fetchall()

        columns_timetravel = [desc[0] for desc in cursor.description]

   

        # Create a DataFrame from the fetched rows and column names

        df_autonomousjobstimetravel = pd.DataFrame(timetravel, columns=columns_timetravel)

        cursor.close()

        cnx.close()

        return df_autonomousjobstimetravel

    except Exception as e:

        PepLog(logFlag,'Entered Exit block of CheckinTimeTravelTable function')

        cursor.close()

        cnx.close()

 

def AddToTimeTravel(UniqueFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate,AsofDate):

    #This function adds the identififed new JDs to the time travel table

    try:

        PepLog(logFlag,'Entered AddToTimeTravel function')

 

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

        values = (UniqueFileId, ReqId, Title, Description, MinExp, MaxExp, role, ctc, position, Loc, joiningDate, AsofDate)

        sql = """INSERT INTO pephire_auto.autonomous_job_time_travel VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""       

        PepLog(logFlag,sql)

        cursor.execute(sql,values)

        cnx.commit()

        cursor.close()

        cnx.close()

        return 'success'

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of AddToTimeTravel function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

        return 'Failed'

 

def AddToAutonomousJobs(UniqueFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate):

    #This function adds the identififed new JDs to the autonomous job table

 

    try:

        PepLog(logFlag,'Entered AddToAutonomousJobs function')

 

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

        values = (UniqueFileId, ReqId, Title, Description, MinExp, MaxExp, role, ctc, position, Loc, joiningDate)

        sql = """INSERT INTO pephire_auto.autonomous_job VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        PepLog(logFlag,sql)

        cursor.execute(sql, values)

        cnx.commit()

        cursor.close()

        cnx.close()

        return 'success'

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of AddToAutonomousJobs function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

        return 'Failed'

def CheckFieldsInTimeTravel(UniqueFileId,Description):

    #This function checks if a JD was already present for a different req ID in the same file

    try:

        PepLog(logFlag,'Entered CheckFieldsInTimeTravel function')

 

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

 

        sql = """select * from pephire_auto.autonomous_job_time_travel where UniqueFileID = '"""+UniqueFileId+"""' and JobDesc = '"""+Description+"""'"""

        PepLog(logFlag,sql)

        cursor.execute(sql)

        timetravelFields = cursor.fetchall()

        columns_timetravelFields = [desc[0] for desc in cursor.description]

 

        # Create a DataFrame from the fetched rows and column names

        df_autonomousjobstimetravelFields = pd.DataFrame(timetravelFields, columns=columns_timetravelFields)

        cursor.close()

        cnx.close()

      

        # df_file_details = GetFileDetailsandLocation(UniqueFileId)

        return df_autonomousjobstimetravelFields

   

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of CheckFieldsInTimeTravel function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

        return 'No JDs found for the given file ID'

def compareJD(UniqueFileId):

    #The function returns the list of existing JDs and their respective details for a given file ID

    try:

        PepLog(logFlag,'Entered compareJD function')

 

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

 

        sql = """select * from pephire_auto.autonomous_job_time_travel where UniqueFileID = '"""+UniqueFileId+"""' """

        PepLog(logFlag,sql)

        cursor.execute(sql)

        timetravel = cursor.fetchall()

        columns_timetravel = [desc[0] for desc in cursor.description]

 

        # Create a DataFrame from the fetched rows and column names

        df_autonomousjobstimetravel = pd.DataFrame(timetravel, columns=columns_timetravel)

        cursor.close()

        cnx.close()

        lst_ExistingJds = df_autonomousjobstimetravel['JobDesc'].tolist()

        # df_file_details = GetFileDetailsandLocation(UniqueFileId)

        return lst_ExistingJds

   

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of compareJD function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

        return 'No JDs found for the given file ID'

       

def MoveToQuarantine(UniqueFileId,AsofDate,ReqId,Title,Description,Loc,MinExp,MaxExp,ctc,role,JoiningDate,Pos,user_id,org_id):

    #The function moves a particular JD to the quarantine table

    try:

        PepLog(logFlag,'Entered MoveToQuarantine function')

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

   

        sql = """insert into  pephire_auto.autonomous_quarantine values ('"""+UniqueFileId+"""' ,'"""+str(AsofDate)+"""','"""+ReqId+"""','"""+Description+"""', '"""+Loc+"""','"""+str(MaxExp)+"""','"""+role+"""','"""+Pos+"""','"""+user_id+"""','"""+org_id+"""') """

        PepLog(logFlag,sql)

        cursor.execute(sql)

        cnx.commit()

        cursor.close()

        cnx.close()

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of MoveToQuarantine function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

def CheckReqIDTimeTravelTable(UniqFileId):

    #The function returns the list of reqIds present for a given file id

    try:

        PepLog(logFlag,'Entered  CheckReqIDTimeTravelTable function')

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

   

        sql = """select ReqID from pephire_auto.autonomous_job_time_travel where  UniqueFileID = '"""+UniqueFileId+"""' """

        PepLog(logFlag,sql)

        cursor.execute(sql)

        REQID = cursor.fetchall()

        columns_REQID = [desc[0] for desc in cursor.description]

   

        # Create a DataFrame from the fetched rows and column names

        df_rqintimetravel = pd.DataFrame(REQID, columns=columns_REQID)

        cursor.close()

        cnx.close()

        return df_rqintimetravel

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of CheckReqIDTimeTravelTable function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

 

def SendEmailAlert(user_id,org_id,message):

    #The function sends the result to the respective user

    try:

        PepLog(logFlag,'Entered SendEmailAlert function')

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

   

        sql = """select email from pephire.users where id = '"""+user_id+"""' and  organization_id = '"""+org_id+"""' """

        PepLog(logFlag,sql)

        cursor.execute(sql)

        user_email = cursor.fetchall()

        columns_email = [desc[0] for desc in cursor.description]

   

        # Create a DataFrame from the fetched rows and column names

        df_userEmail = pd.DataFrame(user_email, columns=columns_email)

        cursor.close()

        cnx.close()

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of SendEmailAlert function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

   

     #if a user mail id is not found for the given user, the mail is send to a dedault id  

    if df_userEmail.shape[0]>0:

        PepLog(logFlag,df_userEmail['email'][0])

    else:

        PepLog(logFlag,'No user')

    recepient_mail = 'sanjna@sentientscripts.com'

    # outlook = win32.Dispatch('Outlook.Application')

    outlook = win32com.client.Dispatch("Outlook.Application")

    mail = outlook.CreateItem(0)

    mail.Subject = message

    mail.Body = message

    mail.To = recepient_mail

    mail.Send()

   

    

def AddToFileRunStatus(UniqueFileId):

    #The function adds the last run time of the file

    try:

        PepLog(logFlag,'Entered  AddToFileRunStatus function')

        cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

        cursor = cnx.cursor()

        sql = ''

        sql = """insert into pephire_auto.autonomous_file_run_status values ('"""+UniqueFileId+"""' ,now(),NULL,NULL) """

        PepLog(logFlag,sql)

        cursor.execute(sql)

        cnx.commit()

        cursor.close()

        cnx.close()

    except Exception as e:

        PepLog(logFlag,'Entered the exit block of AddToFileRunStatus function')

        PepLog(logFlag,e)

        cursor.close()

        cnx.close()

 

def RemoveMapping(duplicate_so_numbers,UniqueFileID):

    #Remove mapping from autonmous fits logs table

    #Remove mapping from autonmous fits table

    #Remove from time travel and autonomous jobs table

    try:

        sQry = "delete FROM pephire_auto.autonomous_fits_logs where ReqID = '" +duplicate_so_numbers+ "' and UniqueFileID ='" + UniqueFileID + "'"

        Flag = db_write(sQry,pephire_db_trans,"others")

        sQry = ''

        if Flag !='1':

            print('Delete from autonomous_fits_logs table failed')

        sQry = "delete FROM pephire_auto.autonomous_fits where ReqID = '" +duplicate_so_numbers+ "' and UniqueFileID ='" + UniqueFileID + "'"

        Flag = db_write(sQry,pephire_db_trans,"others")

        sQry = ''

        if Flag !='1':

            print('Delete from autonomous_fits table failed')

        sQry = "delete FROM pephire_auto.autonomous_job_time_travel where ReqID = '" +duplicate_so_numbers+ "' and UniqueFileID ='" + UniqueFileID + "'"

        Flag = db_write(sQry,pephire_db_trans,"others")

        sQry = ''

        if Flag !='1':

            print('Delete from autonomous_job_time_travel table failed')

        sQry = "delete FROM pephire_auto.autonomous_job where ReqID = '" +duplicate_so_numbers+ "' and UniqueFileID ='" + UniqueFileID + "'"

        Flag = db_write(sQry,pephire_db_trans,"others")

        sQry = ''

        if Flag !='1':

            print('Delete from autonomous_job table failed')  

        return True   

    except:

        return False

 

#Execute the JDs that were missed or that failed during the last execution

try:

    PepLog(logFlag,'Started execution of JDs that were missed or that failed during the last execution')

    for index, row in df_file_runstatus.iterrows():

        status = row['Status']

        PepLog(logFlag,status)

        UniqueFileId = row['UniqueFileID']

        PepLog(logFlag,UniqueFileId)

        df_Output= pd.DataFrame()

        if status == 'Fail':

            #if status is failed, get the JD details from autonomous jobs table

            df_autonomousjobsFiltered = df_autonomousjobs[df_autonomousjobs['UniqueFileID'] == UniqueFileId]

            row_JD = pd.DataFrame(columns=df_autonomousjobsFiltered.columns)

            for i, rww in df_autonomousjobsFiltered.iterrows():

                row_JD = row_JD.append(rww, ignore_index=True)

                UniqueFileId = rww['UniqueFileID']

                ReqId = rww['ReqID']

                Title = rww['JobTitle']

                Description = rww['JobDesc']

                Loc = rww['location']

                MinExp = rww['MinExp']

                MaxExp = rww['MaxExp']

                role = rww['Role']

                position =rww['positions']

                ctc = rww['ctc']

                joiningDate = rww['joiningdate']

                # JDFitment(row_JD)

                df_Result = JDFitment(row_JD)

                df_Output = df_Output.append(df_Result)

        else:

            PepLog(logFlag,'All the scheduled jobs have been successfully executed')

            break;

except Exception as e:

    PepLog(logFlag,'Execution failed while checking for JDs that were missed during the last execution')

       

try:       

    for ind, rw in   df_autonomousjob_schedule.iterrows():

        PepLog(logFlag,'Iterrate through autonomous job schedule')

        UniqFileId = rw['job_file_id']

        PepLog(logFlag,UniqFileId)

        user_id = rw ['uid']

        org_id = rw['oid']

       

        #Check if the file already exits in the time travel table

        df_timetravel = CheckinTimeTravelTable(UniqFileId)

       

        if df_timetravel.shape[0] ==0: #If file is not already present in the time travel table

            #Add the JD to autonomous time travel table

            # df_file_details = GetFileDetailsandLocation(UniqueFileId)

            PepLog(logFlag,'Started execution of new file')

            df_file_details_raw = pd.read_excel('C:/Pephire/AutonomousFlowDemo/GDriveShare/GDriveShare/JD_Input.xlsx')

            AsofDate = datetime.now()

             #Check for duplicate reqId

            df_file_details = df_file_details_raw.drop_duplicates(subset='Request ID', keep='last').copy()

            # # Create a list of all duplicate 'SO Num' entries

            # duplicate_so_numbers = df_file_details_raw[df_file_details_raw.duplicated(subset='Request ID', keep='last')]['Request ID'].tolist()

            # #Remove the current mapping

            # RemoveStatus = RemoveMapping(duplicate_so_numbers,UniqFileId)

            # if RemoveStatus == True:

            #     print('Mapping successfully removed')

            # else:

            #     print("Failed to remove the existing mapping ")

            for i, rw in df_file_details.iterrows():

                ReqId = rw['Request ID']

                Title = rw['Job Title']

                Description = rw['Job Description']

                Loc = rw['Location']

                MinExp = rw['Min Experience']

                MaxExp = rw['Max Experience']

                role = rw['Job Role']

                position =rw[' Positions']

                ctc = rw['Offered CTC']

                joiningDate = rw['Joining Date (yyyy-mm-dd)']

               

                AddToTimeTravel(UniqFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate,AsofDate)

                AddToAutonomousJobs(UniqFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate)

                # AddToFileRunStatus(UniqFileId)

           

            

        else:

            #If the unique File Id is an existing one. Check for the below conditions:

            #1. Duplicate Req ID

            #2. Check if any of the mandatory fields are missing

            #3. Same request details for a different req ID - considered as different req

            #4. Same req ID with different details . The latest one is considered.

            #5. If a reqId has been removed from the file, that will not be considered in the execution

            PepLog(logFlag,'Existing File')

            df_reqIds = CheckReqIDTimeTravelTable(UniqFileId)

            lst_reqIds = df_reqIds['ReqID'].tolist()

            df_file_details = pd.read_excel('C:/Pephire/AutonomousFlowDemo/GDriveShare/GDriveShare/JD_Input.xlsx')

            AsofDate = datetime.now()

            #Check for duplicate reqId

            df_file_details = df_file_details_raw.drop_duplicates(subset='Request ID', keep='last').copy()

            # Create a list of all duplicate 'SO Num' entries

            duplicate_so_numbers = df_file_details_raw[df_file_details_raw.duplicated(subset='Request ID', keep='last')]['Request ID'].tolist()

            #Remove the current mapping

            RemoveStatus  = RemoveMapping(duplicate_so_numbers,UniqFileId)

            if RemoveStatus == True:

                print('Mapping successfully removed')

            else:

                print("Failed to remove the existing mapping ")

            #In df_file_details, check if the same req ID is present twice, then remove the first entry.

            for i, rw in df_file_details.iterrows():

                ReqId = rw['Request ID']

                Title = rw['Job Title']

                Description = rw['Job Description']

                Loc = rw['Location']

                MinExp = str(rw['Min Experience'])

                MaxExp = str(rw['Max Experience'])

                role = rw['Job Role']

                position =str(rw[' Positions'])

                ctc = rw['Offered CTC']

                joiningDate = rw['Joining Date (yyyy-mm-dd)']

               

                #Get the reqID and check if it is already present in the list of existing reqIDs

                if ReqId in lst_reqIds:

                    PepLog(logFlag,'Req ID already present')

                    message = 'Req ID already present'

                    SendEmailAlert(user_id,org_id,message)

                    MoveToQuarantine(UniqueFileId,str(AsofDate),ReqId,Title,Description,Loc,MinExp,MaxExp,ctc,role,str(joiningDate),position,user_id,org_id)

                else:

                    #If it is a new req ID, check if any of the mandatory field is blank

                    PepLog(logFlag,'New Req ID')

                    #First Check if any of the mandatory fields is blank for the new Req ID

                    mandatory_fields = [Description, MaxExp, MinExp,Loc,role,position]  # Example list of strings

   

                    is_any_blank = any(s.isspace() or len(s) == 0 for s in mandatory_fields)

                    if is_any_blank == 'True':

                        PepLog(logFlag,'Has blank in mandatory field')

                        message = 'Has blank value in mandatory field'

                        SendEmailAlert(user_id,org_id,message)

                        MoveToQuarantine(UniqueFileId,AsofDate,ReqId,Title,Description,Loc,MinExp,MaxExp,ctc,role,joiningDate,position,user_id,org_id)

                        #Remove the row with blank from the existing dataframe

                    else:
                        
                        AddToTimeTravel(UniqueFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate,AsofDate)

                        AddToAutonomousJobs(UniqueFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate)

                        #Check if the JD already exists in the table. Add only the new / modified files

                        # LstOfExistingJDs = compareJD(UniqueFileId)

                        # if Description in LstOfExistingJDs:

                        #     #Check if Role,Loc,Min Exp and Max Exp also match for the given JD

                        #     df_fieldsinTimeTravel = CheckFieldsInTimeTravel(UniqueFileId,Description)

                        #     if(df_fieldsinTimeTravel['Role'] == role) & (df_fieldsinTimeTravel['MinExp'] == MinExp) & (df_fieldsinTimeTravel['MaxExp'] == MaxExp) & (df_fieldsinTimeTravel['location'] == Loc):

                        #         #Same request present for a different req ID

                        #         PepLog(logFlag,'Existing request with a different req ID')

                        #         message = 'Existing request with a different req ID'

                        #         SendEmailAlert(user_id,org_id,message)

                        #         MoveToQuarantine(UniqueFileId,AsofDate,ReqId,Title,Description,Loc,MinExp,MaxExp,ctc,role,joiningDate,position,user_id,org_id)

                        #         #Remove the row

                        #     else:

                        #         PepLog(logFlag,'Only the Job description matches')

            

                        #         AddToTimeTravel(UniqueFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate,AsofDate)

                        #         AddToAutonomousJobs(UniqueFileId,ReqId,Title,Description,MinExp,MaxExp,role,ctc,position,Loc,joiningDate)

                       

                        

                        # Check if the ReqID already exists in the table. Add only the new / modified files

                        # LstOfExistingJDs = compareJD(UniqueFileId)

                        # if Description in LstOfExistingJDs:

                        #     #Check if Role,Loc,Min Exp and Max Exp also match for the given JD

                        #     df_fieldsinTimeTravel = CheckFieldsInTimeTravel(UniqueFileId,Description)

                        #     if(df_fieldsinTimeTravel['Role'] == role) & (df_fieldsinTimeTravel['MinExp'] == MinExp) & (df_fieldsinTimeTravel['MaxExp'] == MaxExp) & (df_fieldsinTimeTravel['location'] == Loc):

                        #         #Same request present for a different req ID

                        #         PepLog(logFlag,'Existing request with a different req ID')

                        #         message = 'Existing request with a different req ID'

                        #         SendEmailAlert(user_id,org_id,message)

                        #         MoveToQuarantine(UniqueFileId,AsofDate,ReqId,Title,Description,Loc,MinExp,MaxExp,ctc,role,joiningDate,position,user_id,org_id)

                        #         #Remove the row

                        #     else:

                        #         PepLog(logFlag,'Only the Job description matches')

            

                                
   

except Exception as e:

    PepLog(logFlag,'Error in finding new JD request')

 

try:                          

#Get the scheduled Jobs

    PepLog(logFlag,'Get the list of scheduled jobs')

    cnx = pymysql.connect(user=user_val, password=password_val,host=host_val, database=database_val)

    cursor = cnx.cursor()                 

    # df_autonomousjobs = pd.read_excel('AutonomousJob.xlsx')

    sql = ''

    sql = "select * from pephire_auto.autonomous_job"

    PepLog(logFlag,sql)

    cursor.execute(sql)

    autonomousjobstableUdated = cursor.fetchall()

    columns_autonomousjobstableupdated = [desc[0] for desc in cursor.description]

   

    # Create a DataFrame from the fetched rows and column names

    df_autonomousjobsUpdated = pd.DataFrame(autonomousjobstableUdated, columns=columns_autonomousjobstableupdated)

    sql = ''

    sql = "select * from pephire_auto.autonomous_file_run_status"

    PepLog(logFlag,sql)

    cursor.execute(sql)

    filerunstatusUpdated = cursor.fetchall()

    columns_filerunstatusUpdated = [desc[0] for desc in cursor.description]

    sql = ''

    # Create a DataFrame from the fetched rows and column names

    df_file_runstatusUpdated = pd.DataFrame(filerunstatusUpdated, columns=columns_filerunstatusUpdated)

    

    

    cursor.close()

    cnx.close()

 

except Exception as e:

    PepLog(logFlag,'Entered the exit block of Get Scheduled jobs section')

    PepLog(logFlag,e)

    cursor.close()

    cnx.close()

 

try:

    PepLog(logFlag,'Finding the scheduled jobs')

    #Get the job details from autonomous jobs table and merge it with the respective scheduling details

    df_autonomousJobsFreq = pd.merge(df_autonomousjobsUpdated, df_autonomousjob_schedule, left_on='UniqueFileID',right_on ='job_file_id' )

    df_autonomousJobsFreq_scheduled = pd.merge(df_autonomousJobsFreq, df_file_runstatusUpdated, left_on='UniqueFileID',right_on ='UniqueFileID' )

    #if status is null, then the file has not been executed

    filtered_autonomousJobsFreq = df_autonomousJobsFreq_scheduled[df_autonomousJobsFreq_scheduled['Status'].isnull()]

    #Get the rows where NextRun is equal to greater than the present time

    present_time = datetime.now()

    filtered_autonomousJobsNextRun  = filtered_autonomousJobsFreq[(filtered_autonomousJobsFreq['NextRun'] >=present_time) |(filtered_autonomousJobsFreq['NextRun'].isnull()) ]

except Exception as e:

    PepLog(logFlag,'Error in finding scheduled jobs')

try:   

    if filtered_autonomousJobsNextRun.shape[0]>0:

        PepLog(logFlag,'Filtering the scheduled jobs based on the schedule frequency')

    # #Filter the dataframe based on the frequency

        filtered_autonomousJobsNextRunOnce  = filtered_autonomousJobsNextRun[filtered_autonomousJobsNextRun['frequency']=='Once']

        filtered_autonomousJobsNextRunHourly  = filtered_autonomousJobsNextRun[filtered_autonomousJobsNextRun['frequency']=='Hourly']

        filtered_autonomousJobsNextRunDaily  = filtered_autonomousJobsNextRun[filtered_autonomousJobsNextRun['frequency']=='Daily']

        filtered_autonomousJobsNextRunWeekly  = filtered_autonomousJobsNextRun[filtered_autonomousJobsNextRun['frequency']=='Weekly']

        filtered_autonomousJobsNextRunMonthly  = filtered_autonomousJobsNextRun[filtered_autonomousJobsNextRun['frequency']=='Monthly']

        filtered_autonomousJobsNextRunYearly  = filtered_autonomousJobsNextRun[filtered_autonomousJobsNextRun['frequency']=='Yearly']

   

        df_UniqueId = pd.DataFrame()

        df_nextRun = pd.DataFrame()

        row_df = pd.DataFrame(columns=filtered_autonomousJobsNextRunOnce.columns)

        #Get the files scheduled once

        PepLog(logFlag,'Filtering the  jobs which are scheduled once')

        for index, row in filtered_autonomousJobsNextRunOnce.iterrows():

            UniqueFileID,uid,oid ='','',''

            NextRun=''

            row_df = row_df.append(row, ignore_index=True)

            UniqueFileID = row['UniqueFileID']

            uid = row['uid']

            oid = row['oid']

            current_datetime = datetime.now()

            reqID = filtered_autonomousJobsNextRunOnce['ReqID']

            #Add the unique file ID to a dataframe

            df_UniqueId['UniqueId'] = [UniqueFileID]

            PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled once')

            # row_df['NextRun'] = [NextRun]

            # JDFitment(row_df)

            #Do the fitment of the JD against the existing resumes

            df_nextRun['UniqueId'] = [UniqueFileID]

            df_nextRun['NextRun'] = [NextRun]

            df_nextRun['uid'] = [uid]

            df_nextRun['oid'] = [oid]

           

        #Get the files scheduled hourly  

        PepLog(logFlag,'Filtering the  jobs which are scheduled hourly')

        for i, rw in filtered_autonomousJobsNextRunHourly.iterrows():

            UniqueFileID,uid,oid ='','',''

            UniqueFileID = rw['UniqueFileID']

            uid = rw['uid']

            oid = rw['oid']

            current_datetime = datetime.now()

            one_hour_ago = current_datetime - timedelta(hours=1)

            lastrun_time = GetLastRunTime(UniqueFileID)

            NextRun = ''

            #Check if it is the first run of the given file

            if lastrun_time is None:

                df_UniqueId['UniqueId'] = [UniqueFileID]

                PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled hourly')

                NextRun = current_datetime + timedelta(hours=1)

                df_nextRun['UniqueId'] = [UniqueFileID]

                df_nextRun['NextRun'] = [NextRun]

                df_nextRun['uid'] = [uid]

                df_nextRun['oid'] = [oid]

            else:   

                #check if it has been 1 hour since the last run

                if lastrun_time >= one_hour_ago and lastrun_time <= current_datetime:

                    break;

               

                else:

                    NextRun = ''

                    df_UniqueId['UniqueId'] = [UniqueFileID]

                    PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled hourly')

                    NextRun = current_datetime + timedelta(hours=1)

                    df_nextRun['UniqueId'] = [UniqueFileID]

                    df_nextRun['NextRun'] = [NextRun]

                    df_nextRun['uid'] = [uid]

                    df_nextRun['oid'] = [oid]

       

        #Get the list of files scheduled daily       

        for i, r in filtered_autonomousJobsNextRunDaily.iterrows():

            UniqueFileID,uid,oid ='','',''

            UniqueFileID = r['UniqueFileID']

            uid = r['uid']

            oid = r['oid']

            lastrun_time = GetLastRunTime(UniqueFileID)

            current_datetime = datetime.now()

            #Check if it is the first execution of the file

            if lastrun_time is None:

                NextRun = ''

                df_UniqueId['UniqueId'] = [UniqueFileID]

                PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled daily')

                NextRun = current_datetime + timedelta(days=1)

                df_nextRun['UniqueId'] = [UniqueFileID]

                df_nextRun['NextRun'] = [NextRun]

                df_nextRun['uid'] = [uid]

                df_nextRun['oid'] = [oid]

            else:

                #If it is not the first execution, get the last run date and the current date.

                lastrun_date = lastrun_time.date()

                today_date =  date.today()

                if today_date == lastrun_date:

                    break;

                  

                else:

                    NextRun = ''

                    df_UniqueId['UniqueId'] = [UniqueFileID]

                    PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled daily')

                    NextRun = current_datetime + timedelta(days=1)

                    df_nextRun['UniqueId'] = [UniqueFileID]

                    df_nextRun['NextRun'] = [NextRun]

                    df_nextRun['uid'] = [uid]

                    df_nextRun['oid'] = [oid]

       

        #Get the list of files scheduled weekly   

        for i, re in filtered_autonomousJobsNextRunWeekly.iterrows():

            UniqueFileID,uid,oid ='','',''

            UniqueFileID = re['UniqueFileID']

            uid = re['uid']

            oid = re['oid']

            current_datetime = datetime.now()

            lastrun_time = GetLastRunTime(UniqueFileID)

            current_day = date.today().strftime("%A")

            NextRun = current_datetime + timedelta(weeks=1)

            run_day = re['weekday']

            today_date =  date.today()

            #Check if it is the first execution of the file

            if lastrun_time is None:

                NextRun = ''

                df_UniqueId['UniqueId'] = [UniqueFileID]

                PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled weekly')

                df_nextRun['UniqueId'] = [UniqueFileID]

                df_nextRun['NextRun'] = [NextRun]

                df_nextRun['uid'] = [uid]

                df_nextRun['oid'] = [oid]

            else:

                #check if the current day is equal to the scheduled day for run and current date has passed the last run date

                lastrun_date = lastrun_time.date()

                if current_day == run_day and today_date > lastrun_date  :

                    NextRun = ''

                    df_UniqueId['UniqueId'] = [UniqueFileID]

                    PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled weekly')

                    NextRun = current_datetime + timedelta(weeks=1)

                    df_nextRun['UniqueId'] = [UniqueFileID]

                    df_nextRun['NextRun'] = [NextRun]

                    df_nextRun['uid'] = [uid]

                    df_nextRun['oid'] = [oid]

                else:

                    break;  

                

                

        for i, rr in filtered_autonomousJobsNextRunMonthly.iterrows():  

                UniqueFileID = rr['UniqueFileID']

                uid = rr['uid']

                oid = rr['oid']

                lastrun_time = GetLastRunTime(UniqueFileID)

                current_date = datetime.now()

                monthday_date = rr['date']

                exe_time  = rr['hour']

                today_date = current_date.day

                today_hour =  current_date.strftime("%H:%M")

                fmt = '%H:%M'

                today_time = datetime.strptime(today_hour, fmt).time()

                exec_time = datetime.strptime(exe_time, fmt).time()

                if lastrun_time is None:

                    NextRun = ''

                    if  monthday_date == str(today_date) and today_time >= exec_time:

                        df_UniqueId['UniqueId'] = [UniqueFileID]

                        PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled weekly')

                        NextRun = current_datetime + relativedelta(months=1)

                        df_nextRun['UniqueId'] = [UniqueFileID]

                        df_nextRun['NextRun'] = [NextRun]

                        df_nextRun['uid'] = [uid]

                        df_nextRun['oid'] = [oid]

                    else:

                        break;

                elif current_date.date() > lastrun_time.date() :

                    if  monthday_date == today_date  and today_time >= exec_time:

                        NextRun = ''

                        df_UniqueId['UniqueId'] = [UniqueFileID]

                        PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled weekly')

                        NextRun = current_datetime + relativedelta(months=1)

                        df_nextRun['UniqueId'] = [UniqueFileID]

                        df_nextRun['NextRun'] = [NextRun]

                        df_nextRun['uid'] = [uid]

                        df_nextRun['oid'] = [oid]

                    else:

                        break;

                else:

                    break;

                   

                    

        for i, rre in filtered_autonomousJobsNextRunYearly.iterrows():  

                UniqueFileID = rre['UniqueFileID']

                uid = rre['uid']

                oid = rre['oid']

                lastrun_time = GetLastRunTime(UniqueFileID)

                current_date = datetime.now()

                current_month = current_date.strftime('%b')

                monthday_month = rre['month']

                monthday_date = rre['date']

                today_date = current_date.day

               

                if lastrun_time is None:

                    if current_month == monthday_month and monthday_date == today_date:

                        NextRun = ''

                        df_UniqueId['UniqueId'] = [UniqueFileID]

                        PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled weekly')

                        NextRun = current_datetime + relativedelta(years=1)

                        df_nextRun['UniqueId'] = [UniqueFileID]

                        df_nextRun['NextRun'] = [NextRun]

                        df_nextRun['uid'] = [uid]

                        df_nextRun['oid'] = [oid]

                    else:

                        break;

                       

                elif current_date > lastrun_time:

                    if current_month == monthday_month and monthday_date == today_date:

                        NextRun = ''

                        df_UniqueId['UniqueId'] = [UniqueFileID]

                        PepLog(logFlag,'The file '+UniqueFileID+ ' is scheduled weekly')

                        NextRun = current_datetime + relativedelta(years=1)

                        df_nextRun['UniqueId'] = [UniqueFileID]

                        df_nextRun['NextRun'] = [NextRun]

                        df_nextRun['uid'] = [uid]

                        df_nextRun['oid'] = [oid]

                    else:

                        break;

                else:

                    break;

        if  df_nextRun.shape[0] > 0:

            df_UniqueAutoJobs = pd.merge(df_nextRun,df_autonomousjobsUpdated,left_on ='UniqueId',right_on = 'UniqueFileID' )

            PepLog(logFlag,df_UniqueAutoJobs)

            # df_Output= pd.DataFrame()

           

            for j, val in  df_UniqueAutoJobs.iterrows():

                  PepLog(logFlag,val)

                  PepLog(logFlag,type(val))

                  df_final = pd.DataFrame(columns=df_UniqueAutoJobs.columns)

                  df_final = df_final.append(val, ignore_index=True)

                  # PepLog(logFlag,df_final)

            #     UniqueFileID = df_UniqueAutoJobs['UniqueFileID']

            #     ReqID = df_UniqueAutoJobs['ReqID']

                  df_Result = JDFitment(df_final)

                  df_Output = df_Output.append(df_Result)

                  # result = JDFitment(df_final)

                  # if result == 'success':

                  #     #Append to AutonomousRunStatus table

                  # else:

                  #     #Append to AutonomousRunStatus table with status failed

        else:

            PepLog(logFlag,'No services scheduled for the current time')         

        

    else:

        PepLog(logFlag,'No Jobs Scheduled for Next Run')       

except Exception as e:

    PepLog(logFlag,'Error in finding the jobs scheduled for next run')

   

    

df_Output.to_excel('Output.xlsx')

 

# Loop through AutonomousRunStatus table and get the status of all the reqid of a unqFileId.Based on which the status

#of Unique file ID is decided

