library(factoextra)
library(dplyr)
library(cluster)


# Import data
data_year <- read.csv("C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food purchases by weight in 2015 (LSOA).csv")
View(data_year)
dataFrameFoodCategory <- select(data_year,
                                f_dairy_eggs_weight,
                                f_oils_sauces_weight,
                                f_fish_meat_weight,
                                f_fruit_veg_weight,
                                f_grains_weight,
                                f_readymade_weight,
                                f_sweets_weight)
rownames(dataFrameFoodCategory) <- data_year$area_id
View(dataFrameFoodCategory)


# Remove outliers
f_dairy_eggs_outliers <- boxplot.stats(dataFrameFoodCategory$f_dairy_eggs_weight)$out
dataFrameFoodCategory$f_dairy_eggs_weight <-
  ifelse(dataFrameFoodCategory$f_dairy_eggs_weight %in% f_dairy_eggs_outliers,
         ave(dataFrameFoodCategory$f_dairy_eggs_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_dairy_eggs_weight)

f_fish_meat_outliers <- boxplot.stats(dataFrameFoodCategory$f_fish_meat_weight)$out
dataFrameFoodCategory$f_fish_meat_weight <-
  ifelse(dataFrameFoodCategory$f_fish_meat_weight %in% f_fish_meat_outliers,
         ave(dataFrameFoodCategory$f_fish_meat_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_fish_meat_weight)

f_fruit_veg_outliers <- boxplot.stats(dataFrameFoodCategory$f_fruit_veg_weight)$out
dataFrameFoodCategory$f_fruit_veg_weight <-
  ifelse(dataFrameFoodCategory$f_fruit_veg_weight %in% f_fruit_veg_outliers,
         ave(dataFrameFoodCategory$f_fruit_veg_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_fruit_veg_weight)

f_oils_sauces_outliers <- boxplot.stats(dataFrameFoodCategory$f_oils_sauces_weight)$out
dataFrameFoodCategory$f_oils_sauces_weight <-
  ifelse(dataFrameFoodCategory$f_oils_sauces_weight %in% f_oils_sauces_outliers,
         ave(dataFrameFoodCategory$f_oils_sauces_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_oils_sauces_weight)

f_readymade_outliers <- boxplot.stats(dataFrameFoodCategory$f_readymade_weight)$out
dataFrameFoodCategory$f_readymade_weight <-
  ifelse(dataFrameFoodCategory$f_readymade_weight %in% f_readymade_outliers,
         ave(dataFrameFoodCategory$f_readymade_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_readymade_weight)

f_grains_outliers <- boxplot.stats(dataFrameFoodCategory$f_grains_weight)$out
dataFrameFoodCategory$f_grains_weight <-
  ifelse(dataFrameFoodCategory$f_grains_weight %in% f_grains_outliers,
         ave(dataFrameFoodCategory$f_grains_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_grains_weight)

f_sweets_outliers <- boxplot.stats(dataFrameFoodCategory$f_sweetsweight)$out
dataFrameFoodCategory$f_sweets_weight <-
  ifelse(dataFrameFoodCategory$f_sweets_weight %in% f_sweets_outliers,
         ave(dataFrameFoodCategory$f_sweets_weight, FUN = function(x) NA),
         dataFrameFoodCategory$f_sweets_weight)

dataFrameFoodCategory <- na.omit(dataFrameFoodCategory)
dataFrameFoodCategoryScale <- scale(dataFrameFoodCategory)


# Find the optimum number of clusters
# Sum of squares within cluster
clusteringArea <- kmeans(dataFrameFoodCategoryScale, 
                         centers = 2, 
                         nstart = 20)
sumOfSquare <- clusteringArea$tot.withinss
allSumOfSquare <- data.frame(2, 
                             sumOfSquare)
colnames(allSumOfSquare) <- c("number_of_clusters", 
                              "sum_of_square")
View(allSumOfSquare)

# Average Silhouette width
silhouetteClusteringArea <- 
  silhouette(clusteringArea$cluster, 
             dist(dataFrameFoodCategoryScale))
silhouetteScore <- fviz_silhouette(silhouetteClusteringArea)
averageSilhouetteWidth <- sum(silhouetteScore$data$sil_width)/nrow(dataFrameFoodCategoryScale)
allAverageSilhouetteWidth <- data.frame(2, 
                                        averageSilhouetteWidth)
colnames(allAverageSilhouetteWidth) <- c("number_of_clusters", 
                                         "average_silhouette_width")
View(allAverageSilhouetteWidth)

# Iterate up to 15 clusters
for (numberOfClusters in 3: 15){
  # Sum of squares within cluster
  clusteringArea <- kmeans(dataFrameFoodCategoryScale, 
                           centers = numberOfClusters, 
                           nstart = 20,
                           iter.max = 20)
  sumOfSquare <- clusteringArea$tot.withinss
  allSumOfSquare <- 
    rbind.data.frame(allSumOfSquare,
                     c(numberOfClusters,
                       sumOfSquare))
  
  # Average Silhouette width
  silhouetteClusteringArea <- 
    silhouette(clusteringArea$cluster, 
               dist(dataFrameFoodCategoryScale))
  silhouetteScore <- fviz_silhouette(silhouetteClusteringArea)
  averageSilhouetteWidth <- sum(silhouetteScore$data$sil_width)/nrow(dataFrameFoodCategoryScale)
  allAverageSilhouetteWidth <- 
    rbind.data.frame(allAverageSilhouetteWidth,
                     c(numberOfClusters,
                       averageSilhouetteWidth))
}

# Plot graphs
# Sum of squares within cluster
ggplot(allSumOfSquare,
       aes(x = factor(number_of_clusters),
           y = sum_of_square)) +
  geom_point() +
  labs(title = "Plot of sum of squares of different number of clusters",
       x = "Number of clusters",
       y = "Sum of squares")

# Average Silhouette width
ggplot(allAverageSilhouetteWidth,
       aes(x = factor(number_of_clusters),
           y = average_silhouette_width)) +
  geom_point() +
  labs(title = "Plot of average Silholuette width of different number of clusters",
       x = "Number of clusters",
       y = "Average Silhouette width")

# Export sums of squares and average Silhouette widths to CSV file
sumOfSquare_SilhouetteWidth <-
  full_join(allSumOfSquare,
            allAverageSilhouetteWidth,
            by = "number_of_clusters")
View(sumOfSquare_SilhouetteWidth)

write.csv(sumOfSquare_SilhouetteWidth, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Clustering/Sums of squares and average Silhouette widths of different number of clusters.csv")


# Perform k-means clustering with 4 clusters
clusteringArea <-
  kmeans(dataFrameFoodCategoryScale, centers =  4, nstart = 20)
fviz_cluster(clusteringArea,
             data = dataFrameFoodCategoryScale,
             main = "Cluster plot of LSOAs based on food purchases by weight",
             labelsize = 0)

# Validate result of clustering with Silhouette scores
silhouetteClusteringArea <- 
  silhouette(clusteringArea$cluster,
             dist(dataFrameFoodCategoryScale))
fviz_silhouette(silhouetteClusteringArea)


# Record and export clustering results to CSV file
dataFrameFoodCategory <-
  dataFrameFoodCategory %>% 
  mutate(area_id = rownames(dataFrameFoodCategory))

allAreaId <- dataFrameFoodCategory$area_id

dataFrameCluster <- 
  data.frame(allAreaId[1],
             clusteringArea[["cluster"]][[allAreaId[1]]])
colnames(dataFrameCluster) <- c("area_id", "cluster")
View(dataFrameCluster)

for (index in 2:length(allAreaId)){
  dataFrameCluster <- 
    rbind.data.frame(dataFrameCluster,
                     c(allAreaId[index],
                       clusteringArea[["cluster"]][[allAreaId[index]]]))
}

dataFrameFoodCategory <-
  full_join(dataFrameFoodCategory,
            dataFrameCluster,
            by="area_id")

dataFrameFoodCategory <-
  dataFrameFoodCategory %>%
  select(area_id,
         cluster,
         f_dairy_eggs_weight:f_sweets_weight)

write.csv(dataFrameFoodCategory, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Clustering/Clustering results (food purchase by weight).csv")

