library(dplyr)
library(ggplot2)


# Plot boxplots to compare between clusters
# Dairy and eggs
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_dairy_eggs_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fraction of dairy and eggs purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

# Fish and meat
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_fish_meat_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fraction of fish and meat purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

# Fruit and vegetable
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_fruit_veg_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fracton of fruit and vegetable purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

# Grains
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_grains_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fraction of grains purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

# Oils and sauces
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_oils_sauces_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fraction of oils and sauces purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

# Readymade
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_readymade_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fraction of readymade purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

# Sweets
ggplot(data = data_cluster,
       mapping = aes(x = factor(cluster),
                     y = f_sweets_weight)) +
  geom_boxplot(fill = c("skyblue", "yellow", "orange", "green")) +
  labs(title = "Boxplots of fraction of sweets purchase of different clusters",
       x = "Cluster",
       y = "Fraction")

