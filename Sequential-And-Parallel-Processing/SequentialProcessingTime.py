import timeit
import pandas
import matplotlib.pyplot as plt


# Using functions from other files
from GenerateTransactionRecords import generateTransactionsToDatabase
from ProcessTransactionsGeneratedSequentially import processTransactionsSequentially


def sequentialProcessing():
    processTransactionsSequentially(2022, 9, 10)


if __name__ == "__main__":
    # Holds the experiment results
    timeResults = {
        "number_of_transactions": [],
        "time": []
    }

    # Experiment using number of transactions 10000, 20000, 30000, 40000, 50000 and 60000
    numberOfTransactions = 10000
    while numberOfTransactions <= 60000:
        generateTransactionsToDatabase(10000, 2022, 9, 10)
        time = timeit.timeit(stmt=sequentialProcessing, number=1)
        timeResults["number_of_transactions"].append(numberOfTransactions)
        timeResults["time"].append(time)
        numberOfTransactions = numberOfTransactions + 10000

    # Visualize the experiment result using line plot
    xPoints = timeResults["number_of_transactions"]
    yPoints = timeResults["time"]

    plt.plot(xPoints, yPoints, marker='o')
    plt.title("Line plot of sequential processing time of different number of transactions")
    plt.xlabel("Number of transactions")
    plt.ylabel("Processing time")

    plt.show()

    # Write experiment results to an external file for future reference
    timeResultsDataFrame = pandas.DataFrame(timeResults)
    timeResultsDataFrame.to_csv(
        "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Sequential-And-Parallel-Processing/Time analysis results/Sequential processing time.csv")


