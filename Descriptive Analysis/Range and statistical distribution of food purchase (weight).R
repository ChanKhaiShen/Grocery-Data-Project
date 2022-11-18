library(dplyr)
library(ggplot2)


# Ranges of fractions of purchase of food of different categories based on weight
data_food_weight <- read.csv("C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food purchases by weight in 2015 (LSOA).csv")
View(data_food_weight)

data_food_weight_combined <-
  data.frame(NA, 
             NA)
colnames(data_food_weight_combined) <- c("category", "fraction")
View(data_food_weight_combined)

for (row in data_food_weight$f_dairy_eggs_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Dairy and eggs",
                       row))
}

for (row in data_food_weight$f_fruit_veg_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Fruit and vegetable",
                       row))
}

for (row in data_food_weight$f_fish_meat_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Fish and meat",
                       row))
}

for (row in data_food_weight$f_readymade_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Readymade",
                       row))
}

for (row in data_food_weight$f_grains_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Grains",
                       row))
}

for (row in data_food_weight$f_sweets_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Sweets",
                       row))
}

for (row in data_food_weight$f_oils_sauces_weight){
  data_food_weight_combined <-
    rbind.data.frame(data_food_weight_combined,
                     c("Oils and sauces",
                       row))
}

data_food_weight_combined <- na.omit(data_food_weight_combined)

# Plot boxplots
ggplot(data = data_food_weight_combined,
       mapping = aes(x = category,
                     y = as.numeric(fraction))) +
  geom_boxplot(fill = c("lightblue", "green", "red", "yellow",
                        "orange", "purple", "skyblue")) +
  labs(title = "Boxplots of fractions of food purchase",
       x = "Food category",
       y = "Fraction")


# Plot histograms
# Dairy and eggs
mean_dairy_eggs <- mean(data_food_weight$f_dairy_eggs_weight)
median_dairy_eggs <- median(data_food_weight$f_dairy_eggs_weight)
ggplot(data = data_food_weight, 
         mapping = aes(x = f_dairy_eggs_weight)) +
  geom_histogram(fill = "lightblue",
                 color = "black",
                 binwidth = 0.015) +
  geom_vline(xintercept = mean_dairy_eggs,
             col = "brown") +
  geom_vline(xintercept = median_dairy_eggs,
             col = "purple") +
  annotate("text",
           x = mean_dairy_eggs+0.02, y = 500,
           label = paste("Mean= ", round(mean_dairy_eggs, 4)),
           col = "brown") +
  annotate("text",
           x = median_dairy_eggs-0.02, y = 750,
           label = paste("Median= ", round(median_dairy_eggs, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of dairy and eggs purchase",
       x = "Fraction of dairy and eggs purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.015")

# Fish and meat
mean_fish_meat <- mean(data_food_weight$f_fish_meat_weight)
median_fish_meat <- median(data_food_weight$f_fish_meat_weight)
ggplot(data = data_food_weight, 
         mapping = aes(x = f_fish_meat_weight)) +
  geom_histogram(fill = "green",
                 color = "black",
                 binwidth = 0.015) +
  geom_vline(xintercept = mean_fish_meat,
             col = "brown") +
  geom_vline(xintercept = median_fish_meat,
             col = "purple") +
  annotate("text",
           x = mean_fish_meat-0.02, y = 500,
           label = paste("Mean= ", round(mean_fish_meat, 4)),
           col = "brown") +
  annotate("text",
           x = median_fish_meat+0.02, y = 750,
           label = paste("Median= ", round(median_fish_meat, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of fish and meat purchase",
       x = "Fraction of fish and meat purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.015")

# Fruit and vegetable
mean_fruit_veg <- mean(data_food_weight$f_fruit_veg_weight)
median_fruit_veg <- median(data_food_weight$f_fruit_veg_weight)
ggplot(data = data_food_weight, 
         mapping = aes(x = f_fruit_veg_weight)) +
  geom_histogram(fill = "orange",
                 color = "black",
                 binwidth = 0.02) +
  geom_vline(xintercept = mean_fruit_veg,
             col = "brown") +
  geom_vline(xintercept = median_fruit_veg,
             col = "purple") +
  annotate("text",
           x = mean_fruit_veg+0.025, y = 500,
           label = paste("Mean= ", round(mean_fruit_veg, 4)),
           col = "brown") +
  annotate("text",
           x = median_fruit_veg-0.025, y = 750,
           label = paste("Median= ", round(median_fruit_veg, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of fruit and vegetable purchase",
       x = "Fraction of fruit and vegetable purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.02")

# Grains
mean_grains <- mean(data_food_weight$f_grains_weight)
median_grains <- median(data_food_weight$f_grains_weight)
ggplot(data = data_food_weight,
         mapping = aes(x = f_grains_weight)) +
  geom_histogram(fill = "yellow",
                 color = "black",
                 binwidth = 0.03) +
  geom_vline(xintercept = mean_grains,
             col = "brown") +
  geom_vline(xintercept = median_grains,
             col = "purple") +
  annotate("text",
           x = mean_grains+0.05, y = 500,
           label = paste("Mean= ", round(mean_grains, 4)),
           col = "brown") +
  annotate("text",
           x = median_grains-0.05, y = 750,
           label = paste("Median= ", round(median_grains, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of grains purchase",
       x = "Fraction of grains purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.03")

# Oils and sauces
mean_oils_sauces <- mean(data_food_weight$f_oils_sauces_weight)
median_oils_sauces <- median(data_food_weight$f_oils_sauces_weight)
ggplot(data = data_food_weight,
         mapping = aes(x = f_oils_sauces_weight)) +
  geom_histogram(fill = "green",
                 color = "black",
                 binwidth = 0.0075) +
  geom_vline(xintercept = mean_oils_sauces,
             col = "brown") +
  geom_vline(xintercept = median_oils_sauces,
             col = "purple") +
  annotate("text",
           x = mean_oils_sauces-0.01, y = 500,
           label = paste("Mean= ", round(mean_oils_sauces, 4)),
           col = "brown") +
  annotate("text",
           x = median_oils_sauces+0.01, y = 750,
           label = paste("Median= ", round(median_oils_sauces, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of oils and sauces purchase",
       x = "Fraction of oils and sauces purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.0075")

# Readymade
mean_readymade <- mean(data_food_weight$f_readymade_weight)
median_readymade <- median(data_food_weight$f_readymade_weight)
ggplot(data = data_food_weight,
         mapping = aes(x = f_readymade_weight)) +
  geom_histogram(fill = "yellow",
                 color = "black",
                 binwidth = 0.01) +
  geom_vline(xintercept = mean_readymade,
             col = "brown") +
  geom_vline(xintercept = median_readymade,
             col = "purple") +
  annotate("text",
           x = mean_readymade+0.01, y = 500,
           label = paste("Mean= ", round(mean_readymade, 4)),
           col = "brown") +
  annotate("text",
           x = median_readymade-0.01, y = 750,
           label = paste("Median= ", round(median_readymade, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of readymade purchase",
       x = "Fraction of readymade purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.01")

# Sweets
mean_sweets <- mean(data_food_weight$f_sweets_weight)
median_sweets <- median(data_food_weight$f_sweets_weight)
ggplot(data = data_food_weight,
         mapping = aes(x = f_sweets_weight)) +
  geom_histogram(fill = "skyblue",
                 color = "black",
                 binwidth = 0.015) +
  geom_vline(xintercept = mean_sweets,
             col = "brown") +
  geom_vline(xintercept = median_sweets,
             col = "purple") +
  annotate("text",
           x = mean_sweets+0.025, y = 500,
           label = paste("Mean= ", round(mean_sweets, 4)),
           col = "brown") +
  annotate("text",
           x = median_sweets-0.025, y = 750,
           label = paste("Median= ", round(median_sweets, 4)),
           col = "purple") +
  labs(title = "Histogram of fraction of sweets purchase",
       x = "Fraction of sweets purchase",
       y = "Number of LSOAs",
       caption = "binwidth = 0.015")


# Get details in maximum, minimum and first and third quartiles
distribution_dairy_eggs <- 
  summary(data_food_weight$f_dairy_eggs_weight)
print(distribution_dairy_eggs)
dataFrameDistribution <-
  data.frame("f_dairy_eggs_weight",
             as.numeric(distribution_dairy_eggs[1]),
             as.numeric(distribution_dairy_eggs[2]),
             as.numeric(distribution_dairy_eggs[3]),
             as.numeric(distribution_dairy_eggs[4]),
             as.numeric(distribution_dairy_eggs[5]),
             as.numeric(distribution_dairy_eggs[6]))
colnames(dataFrameDistribution) <-
  c("Category", "Minimum", "First quartile", "Median", 
    "Mean", "Third quartile", "Maximum")
View(dataFrameDistribution)

for (category in c("f_fruit_veg_weight",
                   "f_grains_weight",
                   "f_oils_sauces_weight",
                   "f_sweets_weight",
                   "f_fish_meat_weight",
                   "f_readymade_weight")){
  distribution_category <- 
    summary(data_food_weight[[category]])
  dataFrameDistribution <-
    rbind.data.frame(dataFrameDistribution,
                     c(category,
                       as.numeric(distribution_category[1]),
                       as.numeric(distribution_category[2]),
                       as.numeric(distribution_category[3]),
                       as.numeric(distribution_category[4]),
                       as.numeric(distribution_category[5]),
                       as.numeric(distribution_category[6])))
}

# Write to CSV file for future reference
write.csv(dataFrameDistribution, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Statistical distribution of fraction of food purchase by weight.csv")

