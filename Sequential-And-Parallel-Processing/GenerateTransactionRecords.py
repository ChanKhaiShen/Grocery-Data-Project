import random
import pymongo
import datetime


# List of items that are possible to appear in the transaction generated (total: 50 items)
# There are 17 categories in total. The categories are the same as the Tesco grocery 1.0 dataset.
# Weight is weight of an item in gram. Volume is volume of an item in millilitre.
# The descriptions on the items are not real, but are intended to simulate a real grocery store like Tesco.
listOfAllItems = [
    {
        "item_id": 1,
        "name": "Cabbage",
        "category": "fruit_veg",
        "price": 6.00,
        "weight": 500
    },{
        "item_id": 2,
        "name": "Eggplant",
        "category": "fruit_veg",
        "price": 8.00,
        "weight": 500
    },{
        "item_id": 3,
        "name": "Carrot",
        "category": "fruit_veg",
        "price": 7.00,
        "weight": 400
    },{
        "item_id": 4,
        "name": "Broccoli",
        "category": "fruit_veg",
        "price": 15.00,
        "weight": 300
    },{
        "item_id": 5,
        "name": "Cucumber",
        "category": "fruit_veg",
        "price": 10.00,
        "weight": 750
    },{
        "item_id": 6,
        "name": "Green peas",
        "category": "fruit_veg",
        "price": 3.00,
        "weight": 100
    },{
        "item_id": 7,
        "name": "Corn",
        "category": "fruit_veg",
        "price": 3.00,
        "weight": 300
    },{
        "item_id": 8,
        "name": "Tomato",
        "category": "fruit_veg",
        "price": 5.00,
        "weight": 300
    },{
        "item_id": 9,
        "name": "Pumpkin",
        "category": "fruit_veg",
        "price": 12.00,
        "weight": 400
    },{
        "item_id": 10,
        "name": "Banana",
        "category": "fruit_veg",
        "price": 10.00,
        "weight": 500
    },{
        "item_id": 11,
        "name": "Rambutan",
        "category": "fruit_veg",
        "price": 13.00,
        "weight": 300
    },{
        "item_id": 12,
        "name": "Potato",
        "category": "fruit_veg",
        "price": 7.00,
        "weight": 500
    },{
        "item_id": 13,
        "name": "Papaya",
        "category": "fruit_veg",
        "price": 14.00,
        "weight": 800
    },{
        "item_id": 14,
        "name": "Lemon",
        "category": "fruit_veg",
        "price": 10.00,
        "weight": 300
    },{
        "item_id": 15,
        "name": "Fresh milk",
        "category": "dairy",
        "price": 12.00,
        "weight": 500
    },{
        "item_id": 16,
        "name": "Cheese",
        "category": "dairy",
        "price": 20.00,
        "weight": 500
    },{
        "item_id": 17,
        "name": "Chocolate milk",
        "category": "dairy",
        "price": 15.00,
        "weight": 500
    },{
        "item_id": 18,
        "name": "Butter",
        "category": "dairy",
        "price": 10.00,
        "weight": 300
    },{
        "item_id": 19,
        "name": "Cream",
        "category": "dairy",
        "price": 10.00,
        "weight": 100
    },{
        "item_id": 20,
        "name": "Chicken eggs",
        "category": "eggs",
        "price": 9.00,
        "weight": 300
    },{
        "item_id": 21,
        "name": "Duck eggs",
        "category": "eggs",
        "price": 10.00,
        "weight": 300
    },{
        "item_id": 22,
        "name": "Vegetable oil",
        "category": "fats_oils",
        "price": 10.00,
        "weight": 500
    },{
        "item_id": 23,
        "name": "Frying oil",
        "category": "fats_oils",
        "price": 12.00,
        "weight": 500
    },{
        "item_id": 24,
        "name": "Salt",
        "category": "sauces",
        "price": 5.00,
        "weight": 500
    },{
        "item_id": 25,
        "name": "Sugar",
        "category": "sauces",
        "price": 7.00,
        "weight": 500
    },{
        "item_id": 26,
        "name": "Canned tuna",
        "category": "fish",
        "price": 7.00,
        "weight": 200
    },{
        "item_id": 27,
        "name": "Fish fillet",
        "category": "fish",
        "price": 15.00,
        "weight": 500
    },{
        "item_id": 28,
        "name": "Beef",
        "category": "meat_red",
        "price": 30.00,
        "weight": 1000
    },{
        "item_id": 29,
        "name": "Mutton",
        "category": "meat_red",
        "price": 35.00,
        "weight": 1000
    },{
        "item_id": 30,
        "name": "Chicken",
        "category": "poultry",
        "price": 10.00,
        "weight": 1000
    },{
        "item_id": 31,
        "name": "Duck",
        "category": "poultry",
        "price": 12.00,
        "weight": 1000
    },{
        "item_id": 32,
        "name": "Rice",
        "category": "grains",
        "price": 25.00,
        "weight": 5000
    },{
        "item_id": 33,
        "name": "Flour",
        "category": "grains",
        "price": 3.00,
        "weight": 500
    },{
        "item_id": 34,
        "name": "Bread",
        "category": "grains",
        "price": 3.00,
        "weight": 300
    },{
        "item_id": 35,
        "name": "Biscuit",
        "category": "grains",
        "price": 10.00,
        "weight": 500
    },{
        "item_id": 36,
        "name": "Noodles",
        "category": "grains",
        "price": 10.00,
        "weight": 500
    },{
        "item_id": 37,
        "name": "Cereal",
        "category": "grains",
        "price": 15.00,
        "weight": 1000
    },{
        "item_id": 38,
        "name": "Oat",
        "category": "grains",
        "price": 20.00,
        "weight": 1000
    },{
        "item_id": 39,
        "name": "Udon",
        "category": "grains",
        "price": 12.00,
        "weight": 500
    },{
        "item_id": 40,
        "name": "Sausages",
        "category": "readymade",
        "price": 10.00,
        "weight": 300
    },{
        "item_id": 41,
        "name": "Cheesy wedges",
        "category": "readymade",
        "price": 8.00,
        "weight": 300
    },{
        "item_id": 42,
        "name": "Burger",
        "category": "readymade",
        "price": 5.00,
        "weight": 300
    },{
        "item_id": 43,
        "name": "Candy",
        "category": "sweets",
        "price": 3.00,
        "weight": 50
    },{
        "item_id": 44,
        "name": "Candy",
        "category": "sweets",
        "price": 3.00,
        "weight": 50
    },{
        "item_id": 45,
        "name": "Beer",
        "category": "beer",
        "price": 15.00,
        "volume": 300
    },{
        "item_id": 46,
        "name": "Spirits",
        "category": "spirits",
        "price": 15.00,
        "volume": 300
    },{
        "item_id": 47,
        "name": "Wine",
        "category": "wine",
        "price": 15.00,
        "weight": 300
    },{
        "item_id": 48,
        "name": "Coca cola",
        "category": "soft_drinks",
        "price": 12.00,
        "volume": 600
    },{
        "item_id": 49,
        "name": "Canned Boh tea",
        "category": "tea_coffee",
        "price": 3.00,
        "volume": 300
    },{
        "item_id": 50,
        "name": "Mineral water",
        "category": "water",
        "price": 1.00,
        "volume": 500
    }
]


# Function to generate a dummy transaction record
def generateTransaction(year, month, day):
    # Shop
    # Randomly choose among 45 imaginary shops, namely Shop_1, Shop 2, Shop 3, ... , Shop 45
    shopNumber = random.randint(1, 45)
    shopName = "Shop " + str(shopNumber)

    # Date time
    # Randomly generate a time, assuming the grocery stores are operating 24|7
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dateTime = datetime.datetime(year, month, day, hour, minute, second)

    # Pay method
    # Randomly choose among cash, card and e-wallet
    randomPayMethod = random.randint(1, 3)
    if randomPayMethod == 1:
        payMethod = "cash"
    elif randomPayMethod == 2:
        payMethod = "card"
    else:
        payMethod = "e-wallet"

    # Transaction
    transaction = {
        "shop_name": shopName,
        "date_time": dateTime,
        "pay_method": payMethod,
        "item": []
    }

    # Person
    # Generate an imaginary person of random membership status, and if the person has a
    # membership, the person's gender and age are randomly generated, else the person
    # is just a walk-in person whose gender and age are not recorded
    isMember = random.randint(0, 1)
    if isMember == 1:
        transaction["member"] = "yes"
        gender = random.randint(1, 2)
        if gender == 1:
            transaction["gender"] = "male"
        else:
            transaction["gender"] = "female"
        transaction["age"] = random.randint(20, 80)
    else:
        transaction["member"] = "no"

    # Item
    # Randomly choose at least one item from listOfAllItems and calculate the total price of items chosen
    totalPrice = 0
    id = random.randint(1, 50)
    while id <= 50:
        item = listOfAllItems[id-1]
        transaction["item"].append(item)
        totalPrice = totalPrice + item["price"]
        id = random.randint(1, 70)
    transaction["total_price"] = totalPrice

    return transaction


# Function to generate the required number of transaction records and upload them
# to a cloud database on MongoDB Atlas
def generateTransactionsToDatabase(numberOfTransactions, year, month, day):
    connectionString = "mongodb+srv://User1:user1@cluster0.z5ffg.mongodb.net/test"
    with pymongo.MongoClient(connectionString) as client:
        database = client["MyShop"]
        transactionCollection = database["Transaction"]

        for count in range(numberOfTransactions):
            transaction = generateTransaction(year, month, day)
            transactionCollection.insert_one(transaction)


