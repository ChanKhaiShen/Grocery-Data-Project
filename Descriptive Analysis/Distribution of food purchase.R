library(dplyr)
library(ggplot2)

# Function to draw pie chart for distribution of food purchases.
# Inputs are data, which is the data for drawing the pie chart;
# and title, which is the title of the pie chart.
drawPieChart <- function(data, title){
  data <-
    data %>%
    mutate(mean_percentage = round(as.numeric(mean_fraction)*100, 1))
  
  data <-
    data %>%
    arrange(desc(food_category)) %>%
    mutate(label_y_position = cumsum(mean_percentage) - 0.5*mean_percentage,
           label = paste0(food_category, "\n", mean_percentage, "%"))
  
  ggplot(data,
         aes(x="",
             y=mean_percentage,
             fill=food_category)) +
    geom_bar(stat="identity",
             color="black",
             width=1) +
    geom_text(color="black",
              aes(label=label,
                  y=label_y_position)) +
    coord_polar("y",
                start=0,
                direction=-1) +
    theme_void() +
    theme(legend.position="FALSE") +
    labs(title=title)
}


# Import LSOA grocery data
data_year <- read.csv("C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Grocery Data/year_lsoa_grocery.csv")
View(data_year)

# Food and drinks in 2015
fraction_food_drinks <- select(data_year,
                    area_id,
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
                    
View(fraction_food_drinks)

fraction_food_drinks <-
  fraction_food_drinks %>%
  mutate(f_alcoholic_drinks = f_beer + 
                              f_spirits +
                              f_wine,
         f_non_alcoholic_drinks = f_soft_drinks +
                                  f_tea_coffee +
                                  f_water,
         f_dairy_eggs = f_dairy +
                        f_eggs,
         f_oils_sauces = f_fats_oils +
                         f_sauces,
         f_fish_meat = f_fish +
                       f_meat_red +
                       f_poultry)

write.csv(fraction_food_drinks, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food and drinks purchases in 2015 (LSOA).csv")

# Summary
mean_fraction_food_drinks <-
  data.frame("Alcoholic drinks", mean(fraction_food_drinks$f_alcoholic_drinks))
colnames(mean_fraction_food_drinks) <- c("food_category", "mean_fraction")

View(mean_fraction_food_drinks)

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Dairy and eggs", mean(fraction_food_drinks$f_dairy_eggs)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Fish and meat", mean(fraction_food_drinks$f_fish_meat)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Fruit and vegetable", mean(fraction_food_drinks$f_fruit_veg)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Grains", mean(fraction_food_drinks$f_grains)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Non-alcoholic drinks", mean(fraction_food_drinks$f_non_alcoholic_drinks)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Oils and sauces", mean(fraction_food_drinks$f_oils_sauces)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Readymade", mean(fraction_food_drinks$f_readymade)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Sweets", mean(fraction_food_drinks$f_sweets)))

drawPieChart(mean_fraction_food_drinks, "Pie chart of food and drinks purchase in 2015")

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Beer", mean(fraction_food_drinks$f_beer)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Spirits", mean(fraction_food_drinks$f_spirits)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Wine", mean(fraction_food_drinks$f_wine)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Soft drinks", mean(fraction_food_drinks$f_soft_drinks)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Tea and coffee", mean(fraction_food_drinks$f_tea_coffee)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Water", mean(fraction_food_drinks$f_water)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Dairy", mean(fraction_food_drinks$f_dairy)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Eggs", mean(fraction_food_drinks$f_eggs)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Fats and oils", mean(fraction_food_drinks$f_fats_oils)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Sauces", mean(fraction_food_drinks$f_sauces)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Fish", mean(fraction_food_drinks$f_fish)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Red meat", mean(fraction_food_drinks$f_meat_red)))

mean_fraction_food_drinks <- 
  rbind.data.frame(mean_fraction_food_drinks,
                   c("Poultry", mean(fraction_food_drinks$f_poultry)))

mean_fraction_food_drinks <-
  mean_fraction_food_drinks %>%
  select(category = food_category,
         fraction = mean_fraction)

mean_fraction_food_drinks <-
  mean_fraction_food_drinks %>%
  mutate(percentage = as.numeric(fraction)*100)

write.csv(mean_fraction_food_drinks, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food and drinks purchases in 2015 (Summary).csv")


# Food only in 2015
fraction_food <- select(fraction_food_drinks,
                        area_id,
                        f_dairy_eggs,
                        f_fish_meat,
                        f_fruit_veg,
                        f_grains,
                        f_oils_sauces,
                        f_readymade,
                        f_sweets,
                        f_dairy,
                        f_eggs,
                        f_fats_oils,
                        f_sauces,
                        f_fish,
                        f_meat_red,
                        f_poultry)

View(fraction_food)

fraction_food <-
  fraction_food %>%
  mutate(f_food_total = f_dairy_eggs +
                        f_fish_meat +
                        f_fruit_veg +
                        f_grains +
                        f_oils_sauces +
                        f_readymade +
                        f_sweets)
fraction_food <- 
  fraction_food %>%
  mutate(f_dairy_eggs = f_dairy_eggs / f_food_total,
         f_fish_meat = f_fish_meat / f_food_total,
         f_fruit_veg = f_fruit_veg / f_food_total,
         f_grains = f_grains / f_food_total,
         f_oils_sauces = f_oils_sauces / f_food_total,
         f_readymade = f_readymade / f_food_total,
         f_sweets = f_sweets / f_food_total,
         f_dairy = f_dairy / f_food_total,
         f_eggs = f_eggs / f_food_total,
         f_fats_oils = f_fats_oils / f_food_total,
         f_sauces = f_sauces / f_food_total,
         f_fish = f_fish / f_food_total,
         f_meat_red = f_meat_red / f_food_total,
         f_poultry = f_poultry / f_food_total)

fraction_food <-
  fraction_food %>%
  select(area_id:f_poultry)

write.csv(fraction_food, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food purchases in 2015 (LSOA).csv")

# Summary
mean_fraction_food <-
  data.frame("Dairy and eggs", mean(fraction_food$f_dairy_eggs))
colnames(mean_fraction_food) <- c("food_category", "mean_fraction")

View(mean_fraction_food)

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Fish and meat", mean(fraction_food$f_fish_meat)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Fruit and vegetables", mean(fraction_food$f_fruit_veg)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Grains", mean(fraction_food$f_grains)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Oils and sauces", mean(fraction_food$f_oils_sauces)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Readymade", mean(fraction_food$f_readymade)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Sweets", mean(fraction_food$f_sweets)))

drawPieChart(mean_fraction_food, "Pie chart of food purchase in 2015")

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Dairy", mean(fraction_food$f_dairy)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Eggs", mean(fraction_food$f_eggs)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Fats and oils", mean(fraction_food$f_fats_oils)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Sauces", mean(fraction_food$f_sauces)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Fish", mean(fraction_food$f_fish)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Red meat", mean(fraction_food$f_meat_red)))

mean_fraction_food <-
  rbind.data.frame(mean_fraction_food,
                   c("Poultry", mean(fraction_food$f_poultry)))

mean_fraction_food <-
  mean_fraction_food %>%
  select(category = food_category,
         fraction = mean_fraction)

mean_fraction_food <-
  mean_fraction_food %>%
  mutate(percentage = as.numeric(fraction)*100)

write.csv(mean_fraction_food, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food purchases in 2015 (Summary).csv")


# Food weight in 2015
fraction_food_weight <- select(data_year,
                               area_id,
                               f_dairy_weight,
                               f_eggs_weight,
                               f_fats_oils_weight,
                               f_fish_weight,
                               f_fruit_veg_weight,
                               f_grains_weight,
                               f_meat_red_weight,
                               f_poultry_weight,
                               f_readymade_weight,
                               f_sauces_weight,
                               f_sweets_weight)

View(fraction_food_weight)

fraction_food_weight <-
  fraction_food_weight %>%
  mutate(f_dairy_eggs_weight = f_dairy_weight +
           f_eggs_weight,
         f_oils_sauces_weight = f_fats_oils_weight +
           f_sauces_weight,
         f_fish_meat_weight = f_fish_weight +
           f_meat_red_weight +
           f_poultry_weight)

write.csv(fraction_food_weight, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food purchases by weight in 2015 (LSOA).csv")

# Summary
mean_fraction_food_weight <- 
  data.frame("Dairy and eggs", mean(fraction_food_weight$f_dairy_eggs_weight))
colnames(mean_fraction_food_weight) <- c("food_category", "mean_fraction")

View(mean_fraction_food_weight)

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Fish and meat", mean(fraction_food_weight$f_fish_meat_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Fruit and vegetables", mean(fraction_food_weight$f_fruit_veg_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Grains", mean(fraction_food_weight$f_grains_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Oils and sauces", mean(fraction_food_weight$f_oils_sauces_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Readymade", mean(fraction_food_weight$f_readymade_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Sweets", mean(fraction_food_weight$f_sweets_weight)))

drawPieChart(mean_fraction_food_weight, "Pie chart of food purchase by weight in 2015")

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Dairy", mean(fraction_food_weight$f_dairy_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Eggs", mean(fraction_food_weight$f_eggs_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Fats and oils", mean(fraction_food_weight$f_fats_oils_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Sauces", mean(fraction_food_weight$f_sauces_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Fish", mean(fraction_food_weight$f_fish_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Red meat", mean(fraction_food_weight$f_meat_red_weight)))

mean_fraction_food_weight <- 
  rbind.data.frame(mean_fraction_food_weight,
                   c("Poultry", mean(fraction_food_weight$f_poultry_weight)))

mean_fraction_food_weight <-
  mean_fraction_food_weight %>%
  select(category = food_category,
         fraction = mean_fraction)

mean_fraction_food_weight <-
  mean_fraction_food_weight %>%
  mutate(percentage = as.numeric(fraction)*100)

write.csv(mean_fraction_food_weight, "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Descriptive Analysis/Distribution of food purchases by weight in 2015 (Summary).csv")


