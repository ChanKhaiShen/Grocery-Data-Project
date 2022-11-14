import timeit
import matplotlib.pyplot as plt
import pandas


# Using functions from other files
from GenerateTransactionRecords import generateTransactionsToDatabase
from ProcessTransactionsGeneratedSequentially import processTransactionsSequentially
from ProcessTransactionsGeneratedParallely import processTransactionsParallely


# Functions to process transactions with sequential processing, 2, 3, 4, 5 and 6 threads
def processWithSequentialProcessing():
    processTransactionsSequentially(2022, 9, 22)

def processWith2Threads():
    processTransactionsParallely(2022, 9, 22, 2)

def processWith3Threads():
    processTransactionsParallely(2022, 9, 22, 3)

def processWith4Threads():
    processTransactionsParallely(2022, 9, 22, 4)

def processWith5Threads():
    processTransactionsParallely(2022, 9, 22, 5)

def processWith6Threads():
    processTransactionsParallely(2022, 9, 22, 6)


if __name__ == "__main__":
    # Experiment: Comparison of sequential and parallel processing time and average processing time per transaction
    # record with different number of threads
    # Process 1000000 transactions with sequential processing, 2, 3, 4, 5 and 6 threads

    # Generate 1000000 transactions
    generateTransactionsToDatabase(1000000, 2022, 9, 22)

    # Main record
    allNumberOfThreads = ["Sequential processing", "2 threads", "3 threads", "4 threads", "5 threads", "6 threads"]
    allProcessingTime = []

    # Sequential processing
    time = timeit.timeit(stmt=processWithSequentialProcessing, number=1)
    allProcessingTime.append(time)

    # 2 threads
    time = timeit.timeit(stmt=processWith2Threads, number=1)
    allProcessingTime.append(time)

    # 3 threads
    time = timeit.timeit(stmt=processWith3Threads, number=1)
    allProcessingTime.append(time)

    # 4 threads
    time = timeit.timeit(stmt=processWith4Threads, number=1)
    allProcessingTime.append(time)

    # 5 threads
    time = timeit.timeit(stmt=processWith5Threads, number=1)
    allProcessingTime.append(time)

    # 6 threads
    time = timeit.timeit(stmt=processWith6Threads, number=1)
    allProcessingTime.append(time)

    # Bar chart of processing time of 1000000 transaction records with different number of threads
    plt.bar(allNumberOfThreads, allProcessingTime, color='blue')
    plt.title("Bar chart of processing time of 1000000 transaction records with different number of threads")
    plt.xlabel("Number of threads")
    plt.ylabel("Processing time (second)")
    plt.show()

    # Calculate average processing time per transaction record
    allProcessingTimePerTransaction = []
    for processingTime in allProcessingTime:
        processingTimePerTransaction = processingTime/1000000
        allProcessingTimePerTransaction.append(processingTimePerTransaction)

    # Flush off the first bar chart
    plt.clf()   # figure
    plt.cla()   # axes

    # Bar chart of average processing time per transaction record different number of threads
    plt.bar(allNumberOfThreads, allProcessingTimePerTransaction, color='green')
    plt.title("Bar chart of average processing time per transaction record with different number of threads")
    plt.xlabel("Number of threads")
    plt.ylabel("Average processing time per transaction record (second)")
    plt.show()

    # Write result to external CSV file for future reference
    experimentResult = {
        "number_of_threads": allNumberOfThreads,
        "processing_time": allProcessingTime,
        "processing_time_per_transaction": allProcessingTimePerTransaction
    }
    experimentResultDataFrame = pandas.DataFrame(experimentResult)
    experimentResultDataFrame.to_csv("C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Sequential-And-Parallel-Processing/Time analysis results/Processing time of 1000000 transaction records with different number of threads.csv")


