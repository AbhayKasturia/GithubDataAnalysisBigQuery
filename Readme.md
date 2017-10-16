# Github Archive Data Analysis
Using Google Big Query to calculate the mean and median of the commit message length. Keep updating mean and median on every run to reflect the updated mean and median lengths.

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
Persisting frequencies
