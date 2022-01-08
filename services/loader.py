import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import os
from os import listdir
import mysql.connector
import subprocess
import requests
import ast

def connection():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="meta_data"
    )
    return mydb

def getMetaData(mydb):
    mycursor=mydb.cursor()
    mycursor.execute("SELECT *  FROM load_files")

    myresult = mycursor.fetchall()

    path=[]
    dest=[]
    plugin=[]

    for x in myresult:
        path.append(x[1])
        plugin.append(x[3])
        dest.append(x[4])
    return path,dest,plugin






# Define a worker function
def worker(path,dest,plugin):
    while True:    
            items=listdir(path)
            for item in items:                
                # copyfile(os.path.join(path,item), os.path.join(dest, item))
                command= plugin+' {} {} {} '.format(path,item,dest)
                p = subprocess.Popen(
                    command,
                    shell=True,
                    stdin=None,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    close_fds=True)
                out, err = p.communicate()
                org=uploadToServ(os.path.join(path,item))
                conv=uploadToServ(os.path.join(dest,"_"+item))
                mydb=connection()
                mycursor=mydb.cursor()
                mycursor.execute("insert into uploaded_files (orginal_file,converted_file ,soursID) values (%s,%s,%s)",(org,conv,2))
                mydb.commit()
                mycursor.close()
                os.remove(os.path.join(path,item))
                os.remove(os.path.join(dest,"_"+item))

def uploadToServ(file):
    url='http://localhost:9333/submit'
    files={'files':open(file,'rb')}
    r=requests.post(url,files=files)
    res=ast.literal_eval(r.text)
    return res["fid"]



if __name__ == '__main__':
        mydb=connection()
        pathes,dests,plugins=getMetaData(mydb)
        
        
        # Create as many threads as you want
        threads=[]
        for path,dest,plugin in zip(pathes,dests,plugins):
            threads.append(threading.Thread(target=worker, args = (path,dest,plugin)))
            
        for thread in threads:
            thread.start()



