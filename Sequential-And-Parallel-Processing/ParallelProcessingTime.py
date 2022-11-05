import timeit
import pandas
import matplotlib.pyplot as plt

# Using functions from other files
from GenerateTransactionRecords import generateTransactionsToDatabase
from ProcessTransactionsGeneratedParallely import processTransactionsParallely


# Function to do parallel processing with 2 threads
def parallelProcessing():
    processTransactionsParallely(2022, 9, 17, 2)


if __name__ == "__main__":
    # Record of the experiment results
    timeResults = {
        "number_of_transactions": [],
        "time": []
    }

    # Experiment with 10000, 20000, 30000, 40000, 50000 and 60000 transactions
    numberOfTransactions = 10000
    while numberOfTransactions <= 60000:
        generateTransactionsToDatabase(10000, 2022, 9, 17)
        time = timeit.timeit(stmt=parallelProcessing, number=1)
        timeResults["number_of_transactions"].append(numberOfTransactions)
        timeResults["time"].append(time)
        numberOfTransactions = numberOfTransactions + 10000

    # Visualize the experiment result using line plot
    xPoints = timeResults["number_of_transactions"]
    yPoints = timeResults["time"]

    plt.plot(xPoints, yPoints, marker='o')
    plt.title("Line plot of parallel processing time of different number of transactions using 2 threads")
    plt.xlabel("Number of transactions")
    plt.ylabel("Processing time")

    plt.show()

    # Write experiment results to an external file for future reference
    timeResultsDataFrame = pandas.DataFrame(timeResults)
    timeResultsDataFrame.to_csv(
        "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Sequential-And-Parallel-Processing/Time analysis results/Parallel processing time.csv")


