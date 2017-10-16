from google.cloud import bigquery
from oauth2client.client import GoogleCredentials
import uuid
import pytz
import time
import math
import datetime

# Instantiates a client
client = bigquery.Client(project='evernote-181806')

# Creating global variables to be manipulated during the run of the program.
MEAN_SUM = 0.0
MEAN_COUNT = 0.0
MEAN = 0.0
MEDIAN = 0.0
# Last Run Date
LRD = datetime.datetime.now()

# exec Legacy SQL query on BigQuery
def execQuery(query):
    query = client.run_sync_query('%s' % query)
    query.timeout_ms = 10000
    query.run()

    return(query.rows)

# exec standard SQL query on BigQuery
def execStandardQuery(query):
    query = client.run_sync_query('%s' % query)
    query.timeout_ms = 10000
    query.use_legacy_sql = False
    query.run()

    return(query.rows) 

# priniting result set row wise 
def printResult(rows):    
    print("Total rows = " , len(rows))
    if len(rows) > 0:
        print("Total cols =" , len(rows[0]))
    for row in rows:
        print(row)

# create a new summary dataset if not already existing
def createSummaryDataset(summary_dataset):
    if not summary_dataset.exists():
        summary_dataset.create()
        print('Dataset {} created.'.format("summary"))	
    return summary_dataset

# create a new stats table if not already existing
def createStatTable(stat_table):
    if not stat_table.exists():
        print("Creating Tables")
        # Set the table schema
        stat_table.schema = (
            bigquery.SchemaField('Sum', 'INTEGER'),
            bigquery.SchemaField('Count', 'INTEGER'),
            bigquery.SchemaField('LRD', 'DATETIME'),                       
        )
        stat_table.create()
    return stat_table

# create a new lengthStats table if not already existing
def createLengthStatTable(length_stat_table):
    if not length_stat_table.exists():
        # Set the table schema
        length_stat_table.schema = (
            bigquery.SchemaField('Length', 'INTEGER'),
            bigquery.SchemaField('Count', 'INTEGER')            
        )
        length_stat_table.create()        
    return length_stat_table

# Initialize the dataset and the tables with default values
def initDB(summary_dataset , stat_table, length_stat_table):
    summary_dataset = createSummaryDataset(summary_dataset)
    if not stat_table.exists():
        stat_table = createStatTable(stat_table)
        stat_table.reload()
        while execQuery("SELECT COUNT(*) FROM summary.stats")[0][0] == 0 :    
            time.sleep(2)
            stat_table.reload()
            stat_table.insert_data([(0,0,ct)])        
    length_stat_table = createLengthStatTable(length_stat_table)
    return(summary_dataset, stat_table, length_stat_table)

# Calculating median based on the frequency data
def xMedian(xDict):

    total_elements = sum(xDict.values())
    key_list = list(xDict.keys())
    key_list.sort()    
    # Sorting the frequency dictionary based on the keys    
    if total_elements%2 == 0:
    	# Finding the middle elements if the list size is even
        mid_left_index = total_elements/2-1
        mid_right_index = total_elements/2
        i = 0
        mid_left = -1
        mid_right = -1
        for key in key_list:
            if i + xDict[key] > mid_left_index:
                mid_left = key            
            if i + xDict[key] > mid_right_index:
                mid_right = key            
            if mid_left != -1 and mid_right != -1:
                return((mid_left + mid_right) /2)            
            i = i + xDict[key]
    else:
    	# Finding the middle elements if the list size is odd
        mid_index = math.ceil(total_elements/2 - 1)
        i = 0
        mid = -1
        for key in key_list:
            if i + xDict[key] > mid_index:
                mid = key            
            i = i + xDict[key]            
            if mid != -1:
                return(key)

# Finds the tables to be queried based on the last run date and the curr date
# There will be more than one table if the time period between last run and current has a change in date.
def findQueryTables(last_run_date , curr_date):
    tables = []
    year_change = False
    month_change = False
    day_change = False
    if last_run_date.year < curr_date.year:
        print("YEAR")
        year_change = True        
    
    if last_run_date.month != curr_date.month:
        print("MONTH")
        month_change  = True
    
    if last_run_date.day != curr_date.day:
        print("DAY")
        day_change  = True

    # Assuming that there will be at max 24 hours between two runs. 
    if year_change:
        tables = [str(last_run_date.year)+str(last_run_date.month).zfill(2)+str(last_run_date.day).zfill(2),str(curr_date.year)+"0101"]
    elif month_change:
        tables = [str(last_run_date.year)+str(last_run_date.month).zfill(2)+str(last_run_date.day).zfill(2),str(curr_date.year)+str(curr_date.month).zfill(2)+"01"]
    elif day_change:
        tables = [str(last_run_date.year)+str(last_run_date.month).zfill(2)+str(last_run_date.day).zfill(2),str(curr_date.year)+str(curr_date.month).zfill(2)+str(curr_date.day).zfill(2)]
    else:
        tables =[str(last_run_date.year)+str(last_run_date.month).zfill(2)+str(last_run_date.day).zfill(2)]
    return tables

# Query stats table to get the persisted sum , count and last read date.
def getStats():
	global LRD,MEAN_SUM,MEAN_COUNT
	STAT_QUERY = ('SELECT *'  
	  ' FROM [evernote-181806:summary.stats]'
	  ' GROUP BY 1,2,3')
	res = execQuery(STAT_QUERY)	
	if(len(res) > 0):
	    MEAN_SUM = res[0][0]
	    MEAN_COUNT = res[0][1]
	    LRD = res[0][2].replace(tzinfo=pytz.UTC)
	print("Current LRD" , LRD)

# Querying rqeuired tables in day dataset to get the commit message data of delta time 
def getLengthCounts(tables):
	global LRD,MEAN_SUM,MEAN_COUNT,MEAN
	# For flattening
	DISTINCT_LENGTHS = (
	    '#standardSQL \n'
	    ' CREATE TEMP FUNCTION JsonExtractArray(json STRING) AS ('
	' (SELECT ARRAY_AGG(v IGNORE NULLS)'
	   ' FROM UNNEST(['
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[0].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[1].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[2].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[3].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[4].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[5].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[6].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[7].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[8].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[9].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[10].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[11].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[12].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[13].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[14].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[15].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[16].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[17].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[18].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[19].message"),'
	     ' JSON_EXTRACT_SCALAR(json, "$.commits[20].message")]) AS v)'
	' );')

	# Creating union if there is more than one table
	for table in tables:
	    DISTINCT_LENGTHS = ( DISTINCT_LENGTHS + 
	        ' SELECT length(message) , COUNT(*), MAX(created_at) FROM('
	        ' SELECT JsonExtractArray(payload) as messages , created_at'
	        ' FROM githubarchive.day.' + 
	        table +
	        ' WHERE type="PushEvent" AND created_at > TIMESTAMP("' +
	         str(LRD) +
	        '")) AS commits'
	        ' CROSS JOIN UNNEST(commits.messages) as message'
	        ' GROUP BY length(message)'
	        ' UNION ALL')

	DISTINCT_LENGTHS = DISTINCT_LENGTHS[:len(DISTINCT_LENGTHS)-10] + ';'

	res = execStandardQuery(DISTINCT_LENGTHS)	
	length_counts = {}
	# Inserting the frequency data in length_counts 
	# and calculating the total sum of commit lengths and the total count of commits
	for row in res:
	    if row[0] in length_counts.keys():
	        length_counts[row[0]] = length_count[row[0]] + row[1]
	    else:
	        length_counts[row[0]] = row[1]
	    MEAN_SUM = MEAN_SUM + row[0]*row[1]
	    MEAN_COUNT = MEAN_COUNT + row[1]
	    if LRD < row[2]:
	        LRD = row[2]

	# Calculating mean based on the updated sum and count
	if MEAN_COUNT !=0:
	    MEAN = MEAN_SUM/MEAN_COUNT        

	print("Total distinct lengths are {}".format(len(length_counts)))
	print("Last read date {}".format(LRD))
	return length_counts

# Querying lengthStats to get persisted length counts
def getLengthStats(length_counts):
	global MEDIAN
	LENGTH_COUNTS_QUERY = ('SELECT *'  
  	' FROM [evernote-181806:summary.lengthStats]'
  	' GROUP BY 1,2')

	lc = execQuery(LENGTH_COUNTS_QUERY)	

	# Combining with length_counts generated from the delta data from getLengthCounts
	for row in lc:
	    if row[0] in length_counts.keys():
	        length_counts[row[0]] = length_counts[row[0]] + row[1]
	    else:
	        length_counts[row[0]] = row[1]

	# Calculating median from updated length_counts
	if len(length_counts) > 0 :
	    MEDIAN = xMedian(length_counts)
	else:    
	    MEDIAN =0.0	
	return length_counts

# Updating stats to BigQuery
# Deleting and recreating tables as DELETE fails someitme if their is stream buffer pending
def insertStats(stat_table , length_stat_table, length_counts):	
	global LRD,MEAN_SUM,MEAN_COUNT
	length_stat_table.delete()
	length_stat_table = createLengthStatTable(length_stat_table)
	length_stat_table.reload()	
	# Failover mechanism in case the table inserts are incomplete
	if len(length_counts.items()) > 0 :	    
	    while execQuery("SELECT COUNT(*) FROM summary.lengthStats")[0][0] == 0 :    
	        time.sleep(2)
	        length_stat_table.reload()
	        length_stat_table.insert_data(length_counts.items())

	stat_table.delete()
	stat_table = createStatTable(stat_table)	
	stat_table.reload()	
	# Failover mechanism in case the table inserts are incomplete
	while execQuery("SELECT COUNT(*) FROM summary.stats")[0][0] == 0 :    
	    time.sleep(2)
	    stat_table.reload()
	    stat_table.insert_data([(MEAN_SUM,MEAN_COUNT,LRD)])

	return stat_table , length_stat_table

def main():	
	global LRD,MEAN,MEDIAN
	print("Fetching current time")
	CURRENT_TIME_QUERY = ('SELECT current_timestamp()')
	res = execQuery(CURRENT_TIME_QUERY)
	ct = res[0][0].replace(tzinfo=pytz.UTC).replace(microsecond=0)

	summary_dataset = client.dataset("summary")
	stat_table = summary_dataset.table("stats")  
	length_stat_table = summary_dataset.table("lengthStats")

	print("Creating/Fetching Datasets and tables")
	summary_dataset , stat_table , length_stat_table = initDB(summary_dataset, stat_table ,length_stat_table)
	print("Fetching Stats")	
	getStats()
	
	print("Calculating Tables to query")	
	tables = findQueryTables(LRD,ct)	

	print("Fetching message length counts")	
	length_counts = getLengthCounts(tables)		
	length_counts = getLengthStats(length_counts)	

	print("Updating stats")	
	stat_table, length_stat_table = insertStats(stat_table , length_stat_table, length_counts)

	print('Last Read Date is = {}'.format(LRD))
	print('Updated MEAN is = {}'.format(MEAN))
	print('Updated MEDIAN is = {}'.format(MEDIAN))
  
if __name__ == "__main__":
    main()