This folder contains a Python project to compare sequential processing and parallel processing of raw data.

In the experiments, thousands of dummy transactions of grocery purchases are generated to a cloud database and then fetched to be processed. The processing time is recorded. 

GenerateTransactionRecords.py: 
-generate dummy transaction records

ProcessTransactionsGeneratedSequentially.py: 
-process dummy transaction records sequentially

ProcessTransactionsGeneratedParallely.py: 
-process dummy transaction records parallely with multithreading

SequentialProcessingTime.py: 
-draw line plot of sequential processing time of 10000, 20000, 30000, 40000, 50000 and 60000 transaction records

ParallelProcessingTime.py: 
-draw line plot of parallel processing time (2 threads) of 10000, 20000, 30000, 40000, 50000 and 60000 transaction records

ProcessingTimeOfDifferentNumberOfThreads.py: 
-draw bar chart of processing time of 1000000 transaction records with sequential processing, 2, 3, 4, 5 and 6 threads 
-draw bar chart of average processing time per transaction record with sequential processing, 2, 3, 4, 5 and 6 threads

Dummy analysis results:
-Sample of result of analysis of dummy transaction records 

Time analysis results:
-Result of all experiments, bar charts and line plots
