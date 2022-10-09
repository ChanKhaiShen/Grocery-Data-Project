# Import lsoa grocery data
data_jan <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Jan_lsoa_grocery.csv")
data_feb <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Feb_lsoa_grocery.csv")
data_mar <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Mar_lsoa_grocery.csv")
data_apr <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Apr_lsoa_grocery.csv")
data_may <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/May_lsoa_grocery.csv")
data_jun <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Jun_lsoa_grocery.csv")
data_jul <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Jul_lsoa_grocery.csv")
data_aug <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Aug_lsoa_grocery.csv")
data_sep <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Sep_lsoa_grocery.csv")
data_oct <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Oct_lsoa_grocery.csv")
data_nov <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Nov_lsoa_grocery.csv")
data_dec <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/Dec_lsoa_grocery.csv")
data_year <- read.csv("C:/Users/user/Desktop/Big-Data-Project/7796666/year_lsoa_grocery.csv")
#View(data_year)


library(dplyr)

# Function to make a data frame of only data of certain food 
# category in certain month
make_category_month <- function(data, month, category){
  dataFrame <- data.frame(data$area_id,
                          data[[category]]
  )
  
  colnames(dataFrame) <- c("area_id", 
                           paste(category, month, sep="_"))
  
  return (dataFrame)
}


categories = c("beer", 
               "dairy", 
               "eggs", 
               "fats_oils",
               "fish",
               "fruit_veg", 
               "grains",
               "meat_red", 
               "readymade",
               "poultry",
               "sauces", 
               "soft_drinks", 
               "spirits",
               "sweets", 
               "tea_coffee",
               "water", 
               "wine")

drinks <- c("beer", "soft_drinks", "spirits", "tea_coffee", 
            "water", "wine")

# Restructure food categories related data, combining every month
# into one file and separating different food category into 
# different files
for (category in categories){
  
  # Initialise a data frame for the category
  if (is.element(category, drinks)){   # Drinks don't have weight related data.
    dataFrame_allMonths <- data.frame(data_year$area_id,
                                      data_year$representativeness_norm,
                                      data_year$h_items,
                                      data_year$h_items_norm)
    colnames(dataFrame_allMonths) <- c("area_id", 
                                       "representativeness_norm",
                                       "h_items",
                                       "h_items_norm")
  }
  else{
    dataFrame_allMonths <- data.frame(data_year$area_id,
                                      data_year$representativeness_norm,
                                      data_year$h_items,
                                      data_year$h_items_norm,
                                      data_year$h_items_weight,
                                      data_year$h_items_weight_norm)
    colnames(dataFrame_allMonths) <- c("area_id", 
                                       "representativeness_norm",
                                       "h_items",
                                       "h_items_norm",
                                       "h_items_weight",
                                       "h_items_weight_norm")
  }
  
  # f_{category}
  f_category <- paste("f", category, sep="_")
  dataFrame_jan <- make_category_month(data_jan, "jan", f_category)
  dataFrame_feb <- make_category_month(data_feb, "feb", f_category)
  dataFrame_mar <- make_category_month(data_mar, "mar", f_category)
  dataFrame_apr <- make_category_month(data_apr, "apr", f_category)
  dataFrame_may <- make_category_month(data_may, "may", f_category)
  dataFrame_jun <- make_category_month(data_jun, "jun", f_category)
  dataFrame_jul <- make_category_month(data_jul, "jul", f_category)
  dataFrame_aug <- make_category_month(data_aug, "aug", f_category)
  dataFrame_sep <- make_category_month(data_sep, "sep", f_category)
  dataFrame_oct <- make_category_month(data_oct, "oct", f_category)
  dataFrame_nov <- make_category_month(data_nov, "nov", f_category)
  dataFrame_dec <- make_category_month(data_dec, "dec", f_category)
  
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_jan, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_feb, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_mar, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_apr, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_may, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_jun, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_jul, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_aug, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_sep, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_oct, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_nov, by="area_id")
  dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_dec, by="area_id")
  
  # f_{category}_weight
  if (!is.element(category, drinks)){
    category_weight <- paste(f_category, "weight", sep="_")
    dataFrame_jan <- make_category_month(data_jan, "jan", category_weight)
    dataFrame_feb <- make_category_month(data_feb, "feb", category_weight)
    dataFrame_mar <- make_category_month(data_mar, "mar", category_weight)
    dataFrame_apr <- make_category_month(data_apr, "apr", category_weight)
    dataFrame_may <- make_category_month(data_may, "may", category_weight)
    dataFrame_jun <- make_category_month(data_jun, "jun", category_weight)
    dataFrame_jul <- make_category_month(data_jul, "jul", category_weight)
    dataFrame_aug <- make_category_month(data_aug, "aug", category_weight)
    dataFrame_sep <- make_category_month(data_sep, "sep", category_weight)
    dataFrame_oct <- make_category_month(data_oct, "oct", category_weight)
    dataFrame_nov <- make_category_month(data_nov, "nov", category_weight)
    dataFrame_dec <- make_category_month(data_dec, "dec", category_weight)
    
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_jan, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_feb, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_mar, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_apr, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_may, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_jun, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_jul, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_aug, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_sep, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_oct, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_nov, by="area_id")
    dataFrame_allMonths <- full_join(dataFrame_allMonths, dataFrame_dec, by="area_id")
  }
  
  # Write into .csv file  
  fileName <- paste(category, "lsoa_by_Month.csv", sep="_")
  filePath <- paste("C:/Users/user/Desktop/Big-Data-Project/Food_Categories", fileName, sep="/")
  write.csv(dataFrame_allMonths, filePath)
}
