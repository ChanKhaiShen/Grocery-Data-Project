data_sep <- read.csv("C:/Users/user/Downloads/7796666/Sep_borough_grocery.csv")
dataframe_apr <- data.frame(data_sep)

#Data Processing


#### Function to get rows that exceeded maximum number of NA value columns specified ####
# Returns a list of rows 
getRowsExceededMaximumNAValues <- 
  function(dataframe, maximumNumberOfNAValueColumns){
    numberOfColumns <- ncol(dataframe)
    numberOfNAValues_byRow <- rowSums(is.na(dataframe[,1:numberOfColumns]))

    list_rowsExceededMaximumNAValues <- list()
    currentRow <- 1
    for (numberOfNAValues in numberOfNAValues_byRow){
      if (numberOfNAValues > maximumNumberOfNAValueColumns){
        list_rowsExceededMaximumNAValues <- 
          append(list_rowsExceededMaximumNAValues, currentRow)
        }
      currentRow = currentRow + 1
      }
    
    return (list_rowsExceededMaximumNAValues)
    }


#### Test function ####
#matrix_countNA_row <- matrix(c("row 1", "column 2", "column 3",
#                          "row 2", "column 2", NA,
#                          NA, NA, "column 3",
#                          NA, "column 2", NA,
#                          "row 5", NA, "column 3"), 
#                        nrow=5, ncol=3, byrow=TRUE)
#dataframe_countNA_row <- data.frame(matrix_countNA_row)

#list_rowsMoreThan1NAValue <- 
#  getRowsExceededMaximumNAValues(dataframe_countNA_row, 1)
#for (row in list_rowsMoreThan1NAValue){
#  print(row)
#}
#######################
# Expected result: 3,4

#### Function to get columns that exceeded maximum fraction of NA value specified ####
# Returns a list of columns
# Range of maximumFractionOfNAValue: 0-1
getColumnsExceededMaximumNAValues <-
  function(dataframe, maximumFractionOfNAValue){
    numberOfNAValues_byColumn <- colSums(is.na(dataframe))
    
    totalNumberOfRows <- nrow(dataframe)
    maximumNumberOfNAValues <- 
      totalNumberOfRows * maximumFractionOfNAValue
    
    allColumnNames <- colnames(dataframe)
    list_columnsExceededMaximumNAValues <- list()
    currentColumn <- 1
    for (numberOfNAValues in numberOfNAValues_byColumn){
      if (numberOfNAValues > maximumNumberOfNAValues){
        list_columnsExceededMaximumNAValues <- 
          append(list_columnsExceededMaximumNAValues, 
                 allColumnNames[currentColumn])
      }
      currentColumn <- currentColumn + 1
    }
    
    return (list_columnsExceededMaximumNAValues)
  }


#### Test function ####
#matrix_countNA_column <- matrix(c("row 1", "column 2", NA,
#                          "row 2", NA, NA,
#                          NA, NA, "column 3",
#                          NA, NA, NA,
#                          "row 5", NA, "column 3"), 
#                        nrow=5, ncol=3, byrow=TRUE)
#dataframe_countNA_column <- data.frame(matrix_countNA_column)
#colnames(dataframe_countNA_column) <- c("c1", "c2", "c3") 

#list_columnsMoreThan50PercentNAValue <- 
#  getColumnsExceededMaximumNAValues(dataframe_countNA_column, 0.5)
#for (column in list_columnsMoreThan50PercentNAValue){
#  print(column)
#}
#######################
# Expected result: c2,c3