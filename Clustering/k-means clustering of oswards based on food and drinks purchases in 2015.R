library(factoextra)
library(dplyr)
library(cluster)


# Import data from year_osward_grocery.csv
data_year_osward <- read.csv("C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Grocery Data/year_osward_grocery.csv")
View(data_year_osward)
dataFrameFoodCategory_osward <- select(data_year_osward,
                                       f_beer,
                                       f_dairy,
                                       f_eggs,
                                       f_fats_oils,
                                       f_fish,
                                       f_fruit_veg,
                                       f_grains,
                                       f_meat_red,
                                       f_poultry,
                                       f_readymade,
                                       f_sauces,
                                       f_soft_drinks,
                                       f_spirits,
                                       f_sweets,
                                       f_tea_coffee,
                                       f_water,
                                       f_wine)
rownames(dataFrameFoodCategory_osward) <- data_year_osward$area_id
View(dataFrameFoodCategory_osward)


# Remove outliers
f_beer_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_beer)$out
dataFrameFoodCategory_osward$f_beer <-
  ifelse(dataFrameFoodCategory_osward$f_beer %in% f_beer_outliers,
         ave(dataFrameFoodCategory_osward$f_beer, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_beer)

f_dairy_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_dairy)$out
dataFrameFoodCategory_osward$f_dairy <-
  ifelse(dataFrameFoodCategory_osward$f_dairy %in% f_dairy_outliers,
         ave(dataFrameFoodCategory_osward$f_dairy, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_dairy)

f_wine_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_wine)$out
dataFrameFoodCategory_osward$f_wine <-
  ifelse(dataFrameFoodCategory_osward$f_wine %in% f_wine_outliers,
         ave(dataFrameFoodCategory_osward$f_wine, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_wine)

f_eggs_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_eggs)$out
dataFrameFoodCategory_osward$f_eggs <-
  ifelse(dataFrameFoodCategory_osward$f_eggs %in% f_eggs_outliers,
         ave(dataFrameFoodCategory_osward$f_eggs, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_eggs)

f_fats_oils_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_fats_oils)$out
dataFrameFoodCategory_osward$f_fats_oils <-
  ifelse(dataFrameFoodCategory_osward$f_fats_oils %in% f_fats_oils_outliers,
         ave(dataFrameFoodCategory_osward$f_fats_oils, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_fats_oils)

f_fish_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_fish)$out
dataFrameFoodCategory_osward$f_fish <-
  ifelse(dataFrameFoodCategory_osward$f_fish %in% f_fish_outliers,
         ave(dataFrameFoodCategory_osward$f_fish, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_fish)

f_fruit_veg_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_fruit_veg)$out
dataFrameFoodCategory_osward$f_fruit_veg <-
  ifelse(dataFrameFoodCategory_osward$f_fruit_veg %in% f_fruit_veg_outliers,
         ave(dataFrameFoodCategory_osward$f_fruit_veg, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_fruit_veg)

f_grains_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_grains)$out
dataFrameFoodCategory_osward$f_grains <-
  ifelse(dataFrameFoodCategory_osward$f_grains %in% f_grains_outliers,
         ave(dataFrameFoodCategory_osward$f_grains, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_grains)

f_meat_red_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_meat_red)$out
dataFrameFoodCategory_osward$f_meat_red <-
  ifelse(dataFrameFoodCategory_osward$f_meat_red %in% f_meat_red_outliers,
         ave(dataFrameFoodCategory_osward$f_meat_red, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_meat_red)

f_poultry_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_poultry)$out
dataFrameFoodCategory_osward$f_poultry <-
  ifelse(dataFrameFoodCategory_osward$f_poultry %in% f_poultry_outliers,
         ave(dataFrameFoodCategory_osward$f_poultry, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_poultry)

f_readymade_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_readymade)$out
dataFrameFoodCategory_osward$f_readymade <-
  ifelse(dataFrameFoodCategory_osward$f_readymade %in% f_readymade_outliers,
         ave(dataFrameFoodCategory_osward$f_readymade, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_readymade)

f_water_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_water)$out
dataFrameFoodCategory_osward$f_water <-
  ifelse(dataFrameFoodCategory_osward$f_water %in% f_water_outliers,
         ave(dataFrameFoodCategory_osward$f_water, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_water)

f_tea_coffee_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_tea_coffee)$out
dataFrameFoodCategory_osward$f_tea_coffee <-
  ifelse(dataFrameFoodCategory_osward$f_tea_coffee %in% f_tea_coffee_outliers,
         ave(dataFrameFoodCategory_osward$f_tea_coffee, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_tea_coffee)

f_soft_drinks_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_soft_drinks)$out
dataFrameFoodCategory_osward$f_soft_drinks <-
  ifelse(dataFrameFoodCategory_osward$f_soft_drinks %in% f_soft_drinks_outliers,
         ave(dataFrameFoodCategory_osward$f_soft_drinks, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_soft_drinks)

f_sauces_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_sauces)$out
dataFrameFoodCategory_osward$f_sauces <-
  ifelse(dataFrameFoodCategory_osward$f_sauces %in% f_sauces_outliers,
         ave(dataFrameFoodCategory_osward$f_sauces, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_sauces)

f_spirits_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_spirits)$out
dataFrameFoodCategory_osward$f_spirits <-
  ifelse(dataFrameFoodCategory_osward$f_spirits %in% f_spirits_outliers,
         ave(dataFrameFoodCategory_osward$f_spirits, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_spirits)

f_sweets_outliers <- boxplot.stats(dataFrameFoodCategory_osward$f_sweets)$out
dataFrameFoodCategory_osward$f_sweets <-
  ifelse(dataFrameFoodCategory_osward$f_sweets %in% f_sweets_outliers,
         ave(dataFrameFoodCategory_osward$f_sweets, FUN = function(x) NA),
         dataFrameFoodCategory_osward$f_sweets)

dataFrameFoodCategory_osward <- na.omit(dataFrameFoodCategory_osward)

dataFrameFoodCategoryScale_osward <- scale(dataFrameFoodCategory_osward)


# Find the optimum number of clusters
kmeans(dataFrameFoodCategoryScale_osward, centers = 2, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 3, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 4, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 5, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 6, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 7, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 8, nstart = 20)
kmeans(dataFrameFoodCategoryScale_osward, centers = 9, nstart = 20)

# Perform k-means clustering with 7 clusters
clusteringArea <-
  kmeans(dataFrameFoodCategoryScale_osward, centers =  7, nstart = 20)
fviz_cluster(clusteringArea,
             data = dataFrameFoodCategoryScale_osward,
             main = "Cluster plot of oswards based on food and drinks purchases in 2015",
             labelsize = 0)

# Validate result of clustering with Silhouette scores
silhouetteClusteringArea <- 
  silhouette(clusteringArea$cluster,
             dist(dataFrameFoodCategoryScale_osward))
fviz_silhouette(silhouetteClusteringArea)


# Combine food and drinks purchases, basic information of osward and cluster in data_oswars_cluster
View(clusteringArea)

data_osward_clustered <- select(dataFrameFoodCategory_osward,
                                f_beer:f_wine)
View(data_osward_clustered)

data_osward_clustered <-
  data_osward_clustered %>% 
  mutate(area_id = rownames(data_osward_clustered))

data_osward_clustered <-
  data_osward_clustered %>%
  select(area_id, f_beer:f_wine)

allAreaId <- data_osward_clustered$area_id

dataFrameCluster <- 
  data.frame(allAreaId[1],
             clusteringArea[["cluster"]][[allAreaId[1]]])
View(dataFrameCluster)
colnames(dataFrameCluster) <- c("area_id", "cluster")

for (index in 2:length(allAreaId)){
  dataFrameCluster <- 
    rbind.data.frame(dataFrameCluster,
                     c(allAreaId[index],
                       clusteringArea[["cluster"]][[allAreaId[index]]]))
}

data_osward_clustered <-
  full_join(data_osward_clustered,
            dataFrameCluster,
            by="area_id")

data_osward_information <- 
  select(data_year_osward,
         area_id,
         representativeness_norm,
         transaction_days,
         num_transactions,
         man_day,
         population,
         male,
         female,
         age_0_17,
         age_18_64,
         age_65.,
         avg_age,
         area_sq_km,
         people_per_sq_km)

data_osward_clustered <-
  left_join(data_osward_clustered,
            data_osward_information,
            by="area_id")

data_osward_clustered <-
  data_osward_clustered %>%
  select(area_id, cluster, f_beer:f_wine, representativeness_norm:people_per_sq_km)

data_osward_clustered <-
  data_osward_clustered %>%
  arrange(cluster)

# Export data_osward_cluster to external csv file for future reference
write.csv(data_osward_clustered, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Clustering/osward_7_clusters_based_on_f_category.csv")

