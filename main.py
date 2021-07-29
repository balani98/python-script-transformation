#  **********************SUMMARY OF CODE **********************************
# first we are getting the fields from input and list of companies in array and transforming the input csv to json
# Then with the help of that json and fields array , we are making the new json for output csv file 
# then we convert this new json to csv and dump in anothe file and simultnaeously we convert into dateframe 
# which helps us to push this data  to database
# At the end in case of Success and Error , we push the logs to big query   
# importing csv module
import csv
import json
import configparser
import pandas as pd
import sqlalchemy as db
import datetime
import invokepubsub as ips
# csv file name


# initializing the titles and rows list
fields = []
rows = []
total_rows=0



# Function to convert a CSV to JSON
# Takes the file paths as arguments
def make_json(input_file,fieldnames):
	
    csvfile = open(input_file, 'r')
    jsonArray=[]
    i=0
    #fieldnames = ("FirstName","LastName","IDNumber","Message")
    reader = csv.DictReader( csvfile, fieldnames)
    for row in reader:
        json.dumps(row)
        #jsonfile.write('\n')
        if i!=0:
            jsonArray.append(row)
        i=i+1
    return jsonArray






def analysis():
    # reading csv file
    try:
        json_to_bq={}
        Config = configparser.ConfigParser()
        Config.read('config.ini')
        project_id=Config.get('GENERAL','project_id')
        topic_id=Config.get('pubsub','topic_id')
        json_file_name = Config.get('GENERAL', 'json_file_name')
        db_password=Config.get('DATABASE','db_password')
        public_ipaddress=Config.get('DATABASE','public_ipaddress')
        db_name=Config.get('DATABASE','db_name')
        #creating the connection
        my_conn = db.create_engine("mysql+mysqldb://root:"+db_password+"@"+public_ipaddress+"/"+db_name+"?"+"unix_socket=/cloudsql/companystats")
        #setting service account json file
        input_file = Config.get('FILES', 'input_file')
        output_file = Config.get('FILES','output_file')
        with open(input_file, 'r') as csvfile:
	        # creating a csv reader object
	        csvreader = csv.reader(csvfile)
	        # extracting field names through first row
	        fields = next(csvreader)
            # get total number of rows
	        total_rows=csvreader.line_num
	        print("Total no. of rows: %d"%(csvreader.line_num))
        # making the fields array
        length=len(fields)
        fields_partial=fields[8:length]
        JsonReturnedArray=make_json(input_file,fields)
        newJsonArray=[]
        for jsonObj in JsonReturnedArray:
            for field in fields_partial:
                newjsonObj={}
                newjsonObj['Period']=jsonObj['ï»¿Period']
                newjsonObj['Firm']=jsonObj['Firm']
                newjsonObj['Title']=jsonObj['Title']
                newjsonObj['Publish Date']=jsonObj['Publish Date']
                newjsonObj['Analyst Name']=jsonObj['Analyst Name']
                newjsonObj['Unit']=jsonObj['Unit']
                newjsonObj['Pentagon']=jsonObj['Pentagon']
                newjsonObj['Unit Category']=jsonObj['Unit Category']
                newjsonObj['Competitor']=field
                newjsonObj['Ratings']=jsonObj[field]
                newJsonArray.append(newjsonObj)
        print(newJsonArray)
        data_file = open(output_file, 'w',newline='')
        # create the csv writer object
        csv_writer = csv.writer(data_file)
        # Counter variable used for writing
        # # headers to the CSV file
        count = 0
        # making the json for writing in new csv 
        for loopingjsonObj in newJsonArray:
            if count == 0:
                # Writing headers of CSV file
                header = loopingjsonObj.keys()
                print(header)
                csv_writer.writerow(header)
                count += 1
        # Writing data of CSV file
        csv_writer.writerow(loopingjsonObj.values())
        data_file.close()
        # converting json to dataframe
        df = pd.DataFrame(newJsonArray, columns=['Period','Firm','Title','Publish Date','Analyst Name','Unit','Unit Category',
'Competitor','Ratings'])
    # writing to sql 
       # This table does not have any primary key or unique constraint , Then we need to solve How to
       # track its duplicates 
        df.to_sql(con=my_conn,name='quarter_analysis',if_exists='append',index=False)
        print("table quarter_analysis created")
        try:
            #passing the message to pub sub  for writing to bq in case of success
            json_to_bq["execution_summary"] = "SUCCESS"
            json_to_bq["output"] = newJsonArray
            ips.main(json.dumps(json_to_bq),json_file_name,project_id,topic_id)
        except Exception as e_writing_to_bq:
            print("ERROR : while writing data to bq using PUB_SUB .Error message is "+str(e_writing_to_bq))
    except Exception as exp_main:
        #passing the message to pub sub for writing to bq in case of Error
        json_to_bq["execution_summary"] = "Failed with error: " + str(exp_main)
        json_to_bq["output"] = "Error"
        json_to_bq["time_stamp"]=datetime.datetime.now().isoformat()
        print("Script stopped due to some error :" + str(exp_main))
        try:
            ips.main(json.dumps(json_to_bq),json_file_name,project_id,topic_id)
        except Exception as e_writing_to_bq:
            print("ERROR : while writing data to bq using PUB_SUB .Error message is "+str(e_writing_to_bq))
analysis()