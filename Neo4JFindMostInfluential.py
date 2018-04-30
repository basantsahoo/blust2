from neo4j.v1 import GraphDatabase
from py2neo import Graph 
g = Graph(bolt=True,bolt_port=7687,host="127.0.0.1",password="$trabajo.com")
import numpy as np
import matplotlib.pyplot as plt
import datetime
import pandas as pd

    
def FindDirectConnections():
        q = """
        MATCH (u1:User{region:"bay_area_ca_us"})-[:KNOWS|:IS_COLLEAGUE_OF*1..2]-(u2:User{region:"bay_area_ca_us"}) 
            RETURN u1.user_id as userid,u1.name as uname, 
                   count(u2) as directconnections
			order by directconnections desc
            """
        res = g.data(q)
        
        
        return res
    
def findphoneNosInfluential():
    q = """MATCH (c:Contact)-[:KNOWS]-(u:User)
WHERE u.region = "bay_area_ca_us"
and  not c:User
and not exists(c.spam)
with c as ph, count(u) as directconnections where directconnections>10
RETURN ph.phone_no as phoneno,ph.names[0] as uname, directconnections
order by directconnections desc """    
    res = g.data(q)
        
        
    return res


starttime = datetime.datetime.now()
trivialNos = findphoneNosInfluential()
endtime = datetime.datetime.now()

allNos = []
"""
for ent in trivialNos:
    x = [ent['phone.phone_no'],ent['phone.names'][0],ent['connections']]
    allNos.append(x)
"""
for ent in trivialNos:
    x = [ent['phoneno'],ent['uname'],ent['directconnections']]
    allNos.append(x)

df = pd.DataFrame(allNos)
df.columns = ["User_ID","Name","DirectConnections"]
#df.to_csv("ConnectionsUpto2hops.csv",encoding='utf-8')
df.to_csv("InfluentialPhonenos.csv",encoding='utf-8')

print("starttime "+starttime.strftime("%I:%M%p on %B %d, %Y"))
print("endtime "+endtime.strftime("%I:%M%p on %B %d, %Y"))
print("total run time " + str(endtime-starttime))

