# Github Archive Data Analysis
Using Google Big Query to calculate the mean and median of the commit message length. Keep updating mean and median on every run to reflect the updated mean and median lengths.

# Dataset and documentation
https://www.githubarchive.org/

# Approach
Calculating Mean and Median
1. The commits are a array of json objects inside the payload column. Extract the array of commit
messages from the array of commits.
2. Flatten the array of commit messages , such that each commit message is kept in the same column but
in a new row.
Eg : commit_messages: row1 :['sample1','sample2','sample3'], row2 :['sample4','sample5',sample6']
converted to row1 : 'sample1' , row2 : 'sample2' , .....
3. Group the flattened result set based on message length and aggregate the commit counts for each
length.
4. Use this result set from Step 3 to calculate mean and median.
Calculating Mean and Median over time
5. Persist the values of message lengths and the commit counts from Step 3.
6. Get the max(created_at) from the result set of Step 3 as Last Read Time. Persist the same.
7. Follow steps 1-4 and get data only after Last Read Time.
8. Use the persisted frequencies, combine with frequiencies from Step 7. This will give us the frequencies
since T1(first time the program was run).
9. Data from Step 8 can be used to calculate updated mean and median.

Persisting frequencies and not the whole list of lengths, helps us to optimize cost, performance and scalability,
and still achieve the accuracy required.
Since saving the array of all message lengths(like [(1,1,1,1,3,4,5,6,6,6)] will be huge to keep in memory/db. We
compute frequencies and save the sparse representation of the message lengths, which helps keep the inmemory
utilization to be low.([1:4,3:1,4:1,5:1,6:3]).
A quick analysis on the dataset reveals that there are around 1000-2000 unique message lengths.
SELECT COUNT(*) AS Unique_Lengths FROM(
SELECT JsonExtractArray(payload) as messages
FROM githubarchive.day.20150101
WHERE type="PushEvent") AS commits
CROSS JOIN UNNEST(commits.messages) as message)
GROUP BY length(message)
Thus we don't expect the length-wise count data to have huge number of rows.

# Improvements
1. Based on nuances of the problem to be tackled. We can reduce the storage required for saving the
lengths even further. We can see during the analysis that more than 80% of the commits have length
less than 100 characters. Thus ignoring outliers can further improve our space utilization and should still
provide a good proximate of the mean and median.
2. Based on the problem , it might be useful to log the mean and median stats over time.


# Readme
1. The job is now run adhoc , but can be scheduled as part of a cron job to be run every 6 hours and print
updated mean and median
2. The job requires a service account setup in GCP with access to BigQuery.
3. Also a project with BigQuery API enabled and the project id should be used in the code while creating
client
4. Python 3 and up compatible
