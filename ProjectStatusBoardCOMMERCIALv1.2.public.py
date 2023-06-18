# UpdateNewData watchdog program

#----------
#
# Folder Hierarchy:
#
# Parent folder: 3. Electronic Plan Review (or whatever, not exact names)
#
# 2. NEW COMMERCIAL EPR
#    1 - New Construction
#        [Projects]
#    2 - Finish Out
#        [Projects]
#    3 - Remodel
#        [Projects]
#    4 - Shell
#        [Projects]
#    5 - Other
#        [Projects]
#
# 9- PR Assignments *or new folder!!!*
#    [ReviewerName]
#        1. Approved
#            [Projects]
#        2. Pending
#            [Projects]
#        3. Misc Files
#            [Projects]
#        [Loosefoldersinreview]
#
# Folder Structure - "[BVNumber] - [Note] [Address] - [Project Name] - [City]"
# Parent Folder is either - [Project Type], [Reviewer], Pending, or possibly Approved
# Grandparent Folder is either - "1-New EPR", "9-PR Assig.", or Reviewer
# [Creation Datetime] is in Metadata - will give days in review/days left if countdown
# Mod. datetime/[Update Datetime] is in Metadata - will give "last touched" (until edited on Google Sheets)
#
# Statuses:
#   [Unassigned] if under project type folder
#   [In Review] if under reviewername folder
#   [Pending] if in pending folder
#   [Approved] if in approved folder (if utilizing it)
#   If removed from S-Drive, will be removed from DataFrame/Spreadsheet
#
#----------

import os #imported modules
from datetime import datetime, date, time as t
import pathlib
import ssl
from ssl import SSLError
import time
import re
from pathlib import Path
import pygsheets
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler, DirMovedEvent
from contextlib import suppress
import logging
import sys

#set up base dataframe & directories & logger
column_names = ['Status', 'Created', 'Updated', 'BV Number', 'Address', 'Project Name', 'City', 'Project Type', 'Reviewer']
df = pd.DataFrame(columns = column_names)
df.set_index('BV Number', inplace=True)

#replace "path" with the filepath before running !!
logging.basicConfig(filename='path\\CmmlProjectStatusBoardErrors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

#replace path !!
Assig_Dir = 'path'
NewP_Dir = 'path'

gc = pygsheets.authorize(service_file='./client_secret.json')   #get auth/service file for talking to google

def is_time_between(begin_time, end_time, check_time=None):
    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.now().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else: # crosses midnight
        return check_time >= begin_time or check_time <= end_time

def savefile(msg): #saves dataframe as .csv file with datetime and message at end of filename
    global df   #get global dataframe
    dt = datetime.now() #get current datetime
    #replace path !!
    fpname = 'path\\CmmlProjectStatusBoardBackup ' + dt.strftime('%m-%d-%Y %H%M') + msg + '.csv'    #make filename
    df.to_csv(fpname)   #save as .csv

# def add_cols(dfp):  #adds cols to dataframe for Pending sheet
#     dfp['1st Follow-Up'] = False
#     dfp['2nd Follow-Up'] = False
#     dfp['3rd Follow-Up'] = False
#     return(dfp)

def board_stat(status):
    sh = gc.open('Project Status Board')
    wks = sh.worksheet_by_title('Comments')
    wks.update_value('B3',status) #update the status of the spreadsheet with whatever is passed to this function

def get_df_from_google():   #get the old/current Assigned projects from the spreadsheet
    sh = gc.open('Project Status Board')
    wks2 = sh.worksheet_by_title('Assigned Commercial')
    dfg = wks2.get_as_df(index_column=1)
    return (dfg)

def get_pending_from_google():  #get old/current Pending projects from spreadsheet, including checkboxes
    sh = gc.open('Project Status Board')
    wks = sh.worksheet_by_title('Pending Commercial')
    dfg_pend = wks.get_as_df(index_column=1)
    return (dfg_pend)

def get_fol_parts(Proj_Fol):    #extracts project info from folder name, returns BV number, address, city
    P_PartsR = Proj_Fol.split("-",)   #splits at dash

    P_Parts = [s.strip() for s in P_PartsR] #cleans trailing spaces from each element in list

    BV_Num = (P_Parts[0] + "-" + P_Parts[1])    #combines both parts of BV number
    try:
        P_Addr = (P_Parts[2])   #Project Address
    except: #if can't find address due to delimiter error, or if other error, leave blank
        P_Addr = '' #float("NaN")
    try:
        P_Name = (P_Parts[3])   #Project Name
    except: #if can't find city due to delimiter error, or if other error, leave blank
        P_Name = '' #float("NaN")
    try:
        P_City = (P_Parts[4])   #Project City
    except: #if can't find city due to delimiter error, or if other error, leave blank
        P_City = '' #float("NaN")

    return(BV_Num,P_Addr,P_Name,P_City)    #returns the address, city, and BV number

def get_path_parts(dpath):  #extracts project info from folder path, returns project type, status, reviewer **
    dirps = str(dpath)  #get path as string
    
    dirpp = Path(dpath) #get path as path
    ParFol = (dirpp.parts[-2])  #split into parts to get parent and grandparent folder
    GParFol = (dirpp.parts[-3])

    NewRev = "4. NEW COMMERCIAL EPR"
    Assigned = "5. Commercial PR Assignments"
    
    if NewRev in dirps: #if in "New EPR" folder
        P_Type = ParFol[4:] #the [4:] takes the number off the project type folder so only the text is used
        P_Stat = "Unassigned"
        P_Rev = "Unassigned"

    elif Assigned in dirps: #if in a reviewer's folder
        P_Type = '' #project type empty as it is unknown
        if re.search("Pend", dirps, re.IGNORECASE): #if pending
            P_Stat = "Pending"
            P_Rev = GParFol #reviewer is grandparent folder
        elif re.search("Ready", dirps, re.IGNORECASE):    #if approved
            P_Stat = "Approved"
            P_Rev = GParFol #reviewer is grandparent folder
        else:   #otherwise
            P_Stat = "In Review"
            P_Rev = ParFol    #reviewer is parent folder. check logic!!!
            #case for if in Misc. folder?? 
    else: print("Error")    #come back to this

    return(P_Type,P_Stat,P_Rev)  #returns project type, status, and reviewer

def get_fol_metadata(dpath):
    #gets and formats metadata, returns creation time, modification time
    #when testing, splitting path as in first line gave the wrong creation time, but not splitting gave correct
    #may be different with s-drive. need to test

    #P_Path = os.path.split(dpath)[0] #doesn't seem to be necessary
    P_Path = Path(dpath) #this works during testing. again, not sure since not testing on S-Drive

    try:
        CR_Time_R = os.stat(P_Path).st_ctime   #creation date
        UPD_Time_R = os.stat(P_Path).st_mtime #modification date

        CR = datetime.fromtimestamp(CR_Time_R) #formatting
        CR_Time = CR.strftime('%m/%d/%Y')   #formatting

        UPD = datetime.fromtimestamp(UPD_Time_R)   #formatting
        UPD_Time = UPD.strftime('%m/%d/%Y') #formatting
    except: #if error getting date (not exactly working?)
        CR_Time = 'Error'
        UPD_Time = 'Error'

    return(CR_Time,UPD_Time)    #returns creation time and modification time from folder metadata

def get_dataframe(dpath):   #puts project info in dataframe, returns project dataframe and folder name **
    Proj_Fol = os.path.split(dpath)[1]  #get the folder name from the path

    CR_Time,UPD_Time = get_fol_metadata(dpath)  #calls function to get dates from folder metadata
    P_Type,P_Stat,P_Rev = get_path_parts(dpath) #calls function to get parts from folder path
    BV_Num,P_Addr,P_Name,P_City = get_fol_parts(Proj_Fol)  #calls function to get parts from folder name

    if P_Type == '':    #if variable is empty, set to NaN (so database will view it as emptyu)
        P_Type = float('NaN')

    if P_City == '':
        P_City = float('NaN')

    #make new dataframe with new info
    NProj_Info = {'Status':[P_Stat],'Created':[CR_Time],'Updated':[UPD_Time],'BV Number':[BV_Num],'Address':[P_Addr], 'Project Name':[P_Name],'City':[P_City],'Project Type':[P_Type],'Reviewer':[P_Rev]}
    NProj = pd.DataFrame(NProj_Info)    #put info into dataframe
    NProj.set_index('BV Number', inplace=True) #make sure index is set to BV number

    return NProj,BV_Num #returns bv number and dataframe of new info

def search_and_update(dpath):   #from path: gets project info, searches for project, updates or appends global dataframe, returns updated global dataframe
    global df
    NProj,BV_Num = get_dataframe(dpath) #calls function(s) to get bv number and dataframe formatted project info
    if BV_Num in df.index:  #if project already exists, overwrite with new data
        df.update(NProj)    
    else: df = pd.concat([df,NProj])    #if new project, add row
    
    update_google() #call function to update google sheet

def proj_removed(Proj_Fol): #if project is removed, updates dataframe accordingly - unsure if necessary as projects are removed from dataframe?
    global df   #get dataframe
    UPD = date.today()  #get date
    UPD_Time = UPD.strftime('%m/%d/%Y') #formatting
    #get BV number to search index
    P_PartsR = Proj_Fol.split("-",)   #splits at dash
    P_Parts = [s.strip() for s in P_PartsR] #cleans trailing spaces from each element in list
    BV_Num = (P_Parts[0] + "-" + P_Parts[1])    #combines both parts of BV number

    if BV_Num in df.index:  #search for project to update time and status
        df.at[BV_Num, 'Updated'] = UPD_Time
        df.at[BV_Num, 'Status'] = 'Removed'
    update_google()

def search_if_exists(Proj_Fol):    #search if a project exists (i.e. was moved)
    P_Found = 0 #set found variable to 0 or FALSE
    global Assig_Dir
    global NewP_Dir

    for root,dirs,files in os.walk(Assig_Dir):  #search through assignments folder
        for d in dirs:  #start with directories
            if d.endswith(Proj_Fol):
                dpath = pathlib.PurePath(root+'\\'+str(d))
                P_Found = 1 #if project is found, set found variable to 1 or TRUE

        if P_Found != 1:    #if not found, check for zip folder
            for f in files:
                if f.endswith(Proj_Fol):
                    dpath = pathlib.PurePath(root+'\\'+str(f))
                    P_Found = 1 #if found, set to 1 or TRUE

    for root,dirs,files in os.walk(NewP_Dir):   #search through new projects folders, same as above
        for d in dirs:
            if d.endswith(Proj_Fol):
                dpath = pathlib.PurePath(root+'\\'+str(d))
                P_Found = 1
                
        if P_Found != 1:
            for f in files:
                if f.endswith(Proj_Fol):
                    dpath = pathlib.PurePath(root+'\\'+str(f))
                    P_Found = 1

    if P_Found == 1:    #if project found, update dataframe
        search_and_update(dpath)
    elif P_Found != 1:  #if project not found, update to reflect project removed
        proj_removed(Proj_Fol)

def merge_pend_init(dfpg):  #merges pending info from google sheet to new dataframe on startup
    df_p = get_pending() #gets current pending
#    df_p = add_cols(df_p)   #adds cols
    dfpg.drop(['Status', 'Created', 'Updated', 'Address', 'Project Name', 'City', 'Reviewer'],axis=1,inplace=True)
    df_p.update(dfpg)   #updates from old info
    update_pending(df_p)    #call function to update pending sheet

def merge_pend_live():  #merges pending info from google sheets to new dataframe once project running
    df_p = get_pending() #gets current pending
#    df_p = add_cols(df_p)   #adds cols
    dfpg = get_pending_from_google()    #get current projects from google
    dfpg.drop(['Status', 'Created', 'Updated', 'Address', 'Project Name', 'City', 'Reviewer'],axis=1,inplace=True)
    df_p.update(dfpg)   #updates from old info
    update_pending(df_p)

def initdatagrab(): #initial search through folders for projects. returns df
    #set directories
    global Assig_Dir
    global NewP_Dir
    dfg = get_df_from_google()  #get current/old lists from spreadsheet
    dfpg = get_pending_from_google()
    global df
    #search city folders
    for root, dirs, files in os.walk(NewP_Dir):      #walk through each folder in new project directory
        for fname in files:
            if fname.endswith(".zip"):  #searching zip files
                fpath = pathlib.PurePath(root+'\\'+str(fname))   #gets path for file if zip
                Proj_Fil = fpath.parts[-1]
                if re.match("20..\-", Proj_Fil):    #if zip begins with BV number, update
                    search_and_update(fpath)    #call function to update google sheet with new project

        rpath = pathlib.PurePath(root+'\\'+str(dirs))   #gets path for directory
        dpath = os.path.split(rpath)[0] #isolates end of path
        Proj_Fol = rpath.parts[-2]  #gets just the folder name
        
        if re.match("20..\-", Proj_Fol):    #if folder begins with BV number, update
            search_and_update(dpath)    #call function to update google sheet with new project

    #search assignment folders - same as above
    for root, dirs, files in os.walk(Assig_Dir):      #walk through each folder in assignments directory, same as above
        for fname in files:
            if fname.endswith(".zip"):
                fpath = pathlib.PurePath(root+'\\'+str(fname))   #gets path for file if zip
                Proj_Fil = fpath.parts[-1]
                if re.match("20..\-", Proj_Fil):
                    search_and_update(fpath)

        rpath = pathlib.PurePath(root+'\\'+str(dirs))   #gets path for directory
        dpath = os.path.split(rpath)[0]
        Proj_Fol = rpath.parts[-2]
        
        if re.match("20..\-", Proj_Fol):    #if folder begins with BV number
            search_and_update(dpath)
    dfg.drop(['Status', 'Created', 'Updated', 'Address', 'Project Name', 'City', 'Reviewer'],axis=1,inplace=True)
    df.update(dfg)  #updates dataframe from google with new dataframe from created from folders
    update_google()  #update google sheets with new info - overwrites
    merge_pend_init(dfpg)   #initial merge of old pending info and new pending info
    savefile(' StartUp')  #save new dataframe as backup

def get_other_projs():  #separates projects that are in review/assigned
    global df
    df_grouped = df.groupby(df.Status)  #group by status column

    try:    #try to put everything with status 'In Review' in it's own dataframe
        df_other_stat = df_grouped.get_group('In Review')
    except: #if error, make empty dataframe
        column_names = ['Status', 'Created', 'Updated', 'BV Number', 'Address', 'Project Name', 'City', 'Project Type', 'Reviewer']
        df_other_stat = pd.DataFrame(columns = column_names)
        df_other_stat.set_index('BV Number', inplace=True)

    df_other_stat = df_other_stat.fillna('')    #makes all cells with "NaN" blank
    df_other_stat = df_other_stat.sort_values(by='Created') #sort by created column

    return df_other_stat    #returns projects currently in review as a dataframe

def get_pending():  #separate out projects that are in pending
    global df 
    df_grouped = df.groupby(df.Status)  #group by status column
    
    try:
        df_pending = df_grouped.get_group('Pending')  #get all with status 'Pending'

    except: #if none or if error, create empty dataframe
        column_names = ['Status', 'Created', 'Updated', 'BV Number', 'Address', 'Project Name', 'City', 'Project Type', 'Reviewer']
        df_pending = pd.DataFrame(columns = column_names)
        df_pending.set_index('BV Number', inplace=True)
    
    df_pending = df_pending.fillna('')    #makes all cells with "NaN" blank
    df_pending = df_pending.sort_values(by='Created') #sort by created column

    return df_pending   #returns all projects in pending as dataframe

def get_unassigned_projs(): #separates projects that are unassigned
    global df
    df_grouped = df.groupby(df.Status)  #group by status column
    
    try:
        df_unassigned = df_grouped.get_group('Unassigned')  #get all with status 'Unassigned'

    except: #if error, make empty dataframe
        column_names = ['Status', 'Created', 'Updated', 'BV Number', 'Address', 'Project Name', 'City', 'Project Type', 'Reviewer']
        df_unassigned= pd.DataFrame(columns = column_names)
        df_unassigned.set_index('BV Number', inplace=True)
    
    df_unassigned = df_unassigned.fillna('')    #makes all cells with "NaN" blank
    df_unassigned = df_unassigned.sort_values(by='Created') #sort by created column
    
    return df_unassigned    #return all unassigned projects as dataframe

def update_google():    #update google sheet
    df_unassigned = get_unassigned_projs()  #get only unassigned projects as dataframe
    df_other_stat = get_other_projs()   #get only assigned projects/projects in review
    #df_pend = get_pending()
    
    sh = gc.open('Project Status Board')   #open workbook
    wks1 = sh.worksheet_by_title('Unassigned Commercial') #select worksheet 1
    wks2 = sh.worksheet_by_title('Assigned Commercial')   #select worksheet 2
    wks1.set_dataframe(df_unassigned, start=(1,1), copy_index=True, extend=False, fit=True) #overwrite with new dataframe, removing extra rows - for unassigned projects
    wks2.set_dataframe(df_other_stat, start=(1,1), copy_index=True, extend=False, fit=True) #overwrite with new dataframe, removing extra rows - for assigned projects
    wks1.update_value('A1','BV Number') #add column headers to index columns (for getting info later on reboot/startup)
    wks2.update_value('A1','BV Number')
    merge_pend_live()   #call function to merge pending info on google sheet with new pending info dataframe

def update_pending(df_p):   #update the pending sheet on google
    sh = gc.open('Project Status Board')   #open workbook
    wks_pending = sh.worksheet_by_title('Pending Commercial')
    wks_pending.set_dataframe(df_p, start=(1,1), copy_index=True, extend=False, fit=True) #overwrite with new dataframe, removing extra rows - for pending projects
    wks_pending.update_value('A1','BV Number')  #set col header to BV Number (if this is not done, header will be empty)

class Watcher: # **

    def __init__(self, directory1=Assig_Dir, directory2=NewP_Dir, handler=FileSystemEventHandler()):    #tells the watcher what directory to monitor
        self.observer1 = Observer() #observer 1 setup for assignments
        self.observer2 = Observer() #observer 2 set up for new epr
        self.handler = handler
        self.directory1 = directory1    #directory 1 for observer 1
        self.directory2 = directory2    #directory 2 for observer 2

    def run(self):
        self.observer1.schedule(
            self.handler, self.directory1, recursive=True)
        self.observer1.start()  #starts up first watcher/observer
        print("\nWatcher 1 Running in {}/\n".format(self.directory1))

        self.observer2.schedule(
            self.handler, self.directory2, recursive=True)
        self.observer2.start()  #starts up second water/observer
        print("\nWatcher 2 Running in {}/\n".format(self.directory2))
        board_stat("Running")   #call f(x) to update board status on google sheets

        try:
            while True:
                time.sleep(1)
        except:
            self.observer1.stop()
            self.observer2.stop()
        self.observer1.join()
        self.observer2.join()
        print("\nWatchers Terminated\n")
        board_stat("Offline")
    #below not functioning. fix or remove (5-23-22)
    def stop(self): #not sure if working, trying to work on rebooting and/or soft rebooting
        self.observer1.stop()
        self.observer2.stop()
        self.observer1.join()
        self.observer2.join()
        print("\nWatchers Terminated. Rebooting\n")

class MyHandler(FileSystemEventHandler): # **

    def __init__(self):
        self.last_modified = datetime.now()

    def on_deleted(self, event):      #also triggers if a file/folder is *removed* from the/a directory
        #the following section triggers if a file or folder is deleted/moved/or removed
        #searches for object to determine if moved or deleted. If moved:
        #it checks if the object is a file or folder. if it is a folder it checks if it is in the table
        #if not in the table, a new row is created. If it is in the table, a row is updated based on project number
        #if removed, updates dataframe accordingly
        dpath = event.src_path
        Proj_Fol = os.path.split(dpath)[1]  #isolate folder name from path
        if  re.match("20..\-", Proj_Fol):       #checks if folder name begins with year & dash
                if not re.search("\\.", Proj_Fol) or Proj_Fol.endswith(".zip"): #if it's a folder or zip file, then
                    search_if_exists(Proj_Fol)  #call f(x) to search if project still exists - see if moved or removed

    def on_created(self, event):        #when new file/folder is created; currently triggers if a folder/file is moved into somewhere as well (??)
        #this section below triggers if a folder or file is created
        #if in table, updates the row with new info. If not, a new row is created.
        if event.is_directory or event.src_path.endswith(".zip"):      #if new object is a folder or zip file
            dpath = event.src_path 
            Proj_Fol = os.path.split(dpath)[1]  #isolate folder name

            if  re.match("20..\-", Proj_Fol):   #if folder begins with BV number, will update dataframe
                search_and_update(dpath)    #call f(x) to search if project still exists - if moved or removed

    def on_moved(self, event):      #currently only triggers when folder is renamed
        #this section triggers if a file/folder is *renamed*. If folder, checks if project is on the table
        #if in table, updates the row with new info. If not, a new row is created. 
        if event.is_directory or event.dest_path.endswith(".zip"):      #if new object is a folder or zip file
            dpath = event.dest_path 
            Proj_Fol = os.path.split(dpath)[1]  #isolate folder name

            if  re.match("20..\-", Proj_Fol):   #if folder begins with BV number, will update dataframe
                search_and_update(dpath)    #search if folder exists - moved or removed

if __name__=="__main__": # ** 
    while True: #this should create a loop so the program does some things on an error and then restarts
        try:
            board_stat("Booting...")  #set the status cell on google to "Booting..."
            initdatagrab()  #run initial data grab function
            w = Watcher(NewP_Dir, Assig_Dir, MyHandler())       #tells the watcher what directory to monitor
            w.run() #run watchdog
        except ssl.SSLError as e:
            logger.error(e, stack_info=True, exc_info=True)  #log the error in a text file
            savefile(' Error')  #save the current dataframe as backup
            print("error! restarting")  #print message to terminal
        else: break
    
#print number of folders/.zip in files (reveiwer folders, unassigned, etc) to verify vs number of projects on sheet? 
#only save certain cols when merging - leave reviewer/etc alone (?) in case of change
#not really working - the while loop - try something for loss of wifi?? 


#did not turn itself off BUT did exit without restarting when using keyboard interrupt (at time listed on final if statement). 
#something about the while loop doesn't let it get to the if statement. breaking with keyboard interrupt exits the while statement (???) if time is between x and y (???)