

#Get user type 

def (user_id,event,candidate_id):
    
    sql = 
    
    userType = execute("select user_type from pephire.auto_userTypes where user_id = user_id")
     #Get the function called for the given user and event
     
     sql = "select function from pephire_auto.functions where user_type = and event = "
     
     function = excute(sql)
     call function(candidate_id)
 
    
