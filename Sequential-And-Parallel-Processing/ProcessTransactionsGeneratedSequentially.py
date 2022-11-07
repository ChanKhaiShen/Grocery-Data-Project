import pymongo
import datetime
import pandas
import math
from numpy import NaN
from bson import ObjectId


# Function to process one transaction
def processTransaction(transaction):
    # Skip if item is missing
    if "item" not in transaction.keys():
        return

    # Set isMember flag
    isMember = False
    if "member" in transaction.keys() and type(transaction["member"]) == str:
        member = transaction["member"]
        member = member.strip()
        member = member.lower()
        if member == "yes":
            isMember = True

    # Set isMale or isFemale flag
    isMale = False
    isFemale = False
    if "gender" in transaction.keys() and type(transaction["gender"]) == str:
        gender = transaction["gender"]
        gender = gender.strip()
        gender = gender.lower()
        if gender == "male":
            isMale = True
        elif gender == "female":
            isFemale = True

    # Set age value
    if "age" in transaction.keys() and type(transaction["age"]) == int and 18 <= transaction["age"] <= 150:
        age = transaction["age"]
    else:
        age = 0

    # Set pay method flag
    payCash = False
    payCard = False
    payEWallet = False
    if "pay_method" in transaction.keys() and type(transaction["pay_method"]) == str:
        payMethod =  transaction["pay_method"]
        payMethod = payMethod.strip()
        payMethod = payMethod.lower()
        if payMethod == "cash":
            payCash = True
        elif payMethod == "card":
            payCard = True
        elif payMethod == "e-wallet":
            payEWallet = True

    # Set shop name
    if "shop_name" in transaction.keys() and type(transaction["shop_name"]) == str:
        shopName = transaction["shop_name"]
        shopName = shopName.strip()
    else:
        shopName = None

    for item in transaction["item"]:

        # Skip if item id is missing
        if "item_id" not in item.keys():
            continue

        item_id = item["item_id"]

        # Skip if item_id is NaN
        if type(item_id) == float and math.isnan(item_id):
            continue

        if item_id not in allItems["item_id"]:  # New item
            allItems["item_id"].append(item_id)
            allItems["name"].append(None)
            allItems["price"].append(None)
            allItems["category"].append(None)
            allItems["item_weight"].append(None)
            allItems["item_volume"].append(None)
            allItems["sold"].append(0)
            allItems["member"].append(0)
            allItems["walk_in"].append(0)
            allItems["male"].append(0)
            allItems["female"].append(0)
            allItems["age"].append(0)
            allItems["pay_cash"].append(0)
            allItems["pay_card"].append(0)
            allItems["pay_ewallet"].append(0)

            for shopNumber in range(1, 46):
                shop_Name = "Shop " + str(shopNumber)
                allItems[shop_Name].append(0)

        item_index = allItems["item_id"].index(item_id)

        # Basic information of the item
        if allItems["name"][item_index] is None and "name" in item.keys() and type(item["name"]) == str:
            allItems["name"][item_index] = item["name"]

        if allItems["price"][item_index] is None and "price" in item.keys() and type(item["price"]) in (int, float):
            price = item["price"]
            if not math.isnan(price):
                allItems["price"][item_index] = price

        if allItems["category"][item_index] is None and "category" in item.keys() and type(item["category"]) == str:
            allItems["category"][item_index] = item["category"]

        if allItems["item_weight"][item_index] is None and "weight" in item.keys() and type(item["weight"]) in (int, float):
            weight = item["weight"]
            if not math.isnan(weight):
                allItems["item_weight"][item_index] = weight

        if allItems["item_volume"][item_index] is None and "volume" in item.keys() and type(item["volume"]) in (int, float):
            volume = item["volume"]
            if not math.isnan(volume):
                allItems["item_volume"][item_index] = volume

        # Increment the total number of items sold
        allItems["sold"][item_index] = allItems["sold"][item_index] + 1

        # Increment the total number of member or walk-in person buying the item; if the current
        # person is member, increment the total number of male or female and update the average
        # age of people buying the item
        if not isMember:
            allItems["walk_in"][item_index] = allItems["walk_in"][item_index] + 1

        else:
            allItems["member"][item_index] = allItems["member"][item_index] + 1

            if isMale:
                allItems["male"][item_index] = allItems["male"][item_index] + 1
            elif isFemale:
                allItems["female"][item_index] = allItems["female"][item_index] + 1

            if age != 0:
                averageAge = allItems["age"][item_index]
                if averageAge == 0:
                    allItems["age"][item_index] = age
                else:
                    averageAge = (averageAge + age) / 2
                    allItems["age"][item_index] = averageAge

        # Increment the number of people using the relevant pay method and buying at the relevant shop
        if payCash:
            allItems["pay_cash"][item_index] = allItems["pay_cash"][item_index] + 1
        elif payCard:
            allItems["pay_card"][item_index] = allItems["pay_card"][item_index] + 1
        elif payEWallet:
            allItems["pay_ewallet"][item_index] = allItems["pay_ewallet"][item_index] + 1

        if shopName in allItems.keys():
            allItems[shopName][item_index] = allItems[shopName][item_index] + 1


# Sequential processing
# Function to sequentially process the transaction records of the specified shop name on the specified day
def processTransactionsSequentially(year, month, day):

    # The master record for the whole day
    global allItems
    allItems = {
        "item_id": [],
        "name": [],
        "price": [],
        "category": [],
        "item_weight": [],
        "item_volume": [],
        "sold": [],
        "member": [],
        "walk_in": [],
        "male": [],
        "female": [],
        "age": [],
        "pay_cash": [],
        "pay_card": [],
        "pay_ewallet": []
    }

    for shopNumber in range(1, 46):
        shopName = "Shop " + str(shopNumber)
        allItems[shopName] = []

    connectionString = "mongodb+srv://User1:user1@cluster0.z5ffg.mongodb.net/test"
    with pymongo.MongoClient(connectionString) as client:
        database = client["MyShop"]
        transactionCollection = database["Transaction"]

        for shopNumber in range(1, 46):
            shopName = "Shop " + str(shopNumber)

            for hour in range(0, 24):
                startOfHour = datetime.datetime(year, month, day, hour, 0, 0)
                endOfHour = datetime.datetime(year, month, day, hour, 59, 59)

                for transaction in transactionCollection.find(
                        {"$and": [{"date_time": {"$gte": startOfHour}},  # gte: greater than or equal to
                                  {"date_time": {"$lte": endOfHour}},  # lte: less than or equal to
                                  {"shop_name": str(shopName)}]}):

                    processTransaction(transaction)

    # Write the combined result to CSV file
    allItemsDataFrame = pandas.DataFrame(allItems)
    date = datetime.datetime(year, month, day)
    allItemsFileName = "Items " + date.strftime("%d %b %Y") + ".csv"
    allItemsFilePath = "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Sequential-And-Parallel-Processing/Dummy analysis results/" + allItemsFileName
    allItemsDataFrame.to_csv(allItemsFilePath)


if __name__ == "__main__":
    # Automated test of function processTransaction()

    # processTransaction() will write its result to allItems
    global allItems
    allItems = {
        "item_id": [],
        "name": [],
        "price": [],
        "category": [],
        "item_weight": [],
        "item_volume": [],
        "sold": [],
        "member": [],
        "walk_in": [],
        "male": [],
        "female": [],
        "age": [],
        "pay_cash": [],
        "pay_card": [],
        "pay_ewallet": []
    }

    for shopNumber in range(1, 46):
        shopName = "Shop " + str(shopNumber)
        allItems[shopName] = []

    # Testing NaN value instead of text and numerical value
    transaction = {
        "_id": ObjectId('636625f0c599615f2253d0e7'),
        "shop_name": NaN,
        "date_time": NaN,
        "pay_method": NaN,
        "item": [
            {
                "item_id": 100,
                "name": NaN,
                "category": NaN,
                "price": NaN,
                "weight": NaN,
                "volume": NaN
            },
        ],
        "member": NaN,
        "gender": NaN,
        "age": NaN,
        "total_price": NaN
    }

    # The current allItems as expected
    allItemsExpected = {
        "item_id": [100,],
        "name": [None,],
        "price": [None,],
        "category": [None,],
        "item_weight": [None,],
        "item_volume": [None,],
        "sold": [1,],
        "member": [0,],
        "walk_in": [1,],
        "male": [0,],
        "female": [0,],
        "age": [0,],
        "pay_cash": [0,],
        "pay_card": [0,],
        "pay_ewallet": [0,]
    }

    for shopNumber in range(1, 46):
        shop_Name = "Shop " + str(shopNumber)
        allItemsExpected[shop_Name] = [0,]

    processTransaction(transaction)
    assert allItems == allItemsExpected, "Test for NaN value instead of text and numerical value failed."

    # Testing text value instead of numerical value
    transaction = {
        "_id": ObjectId('636625f0c599615f2253d0e7'),
        "shop_name": "abc",
        "date_time": "abc",
        "pay_method": "cash",
        "item": [
            {
                "item_id": "abc",
                "name": "abc",
                "category": "abc",
                "price": "abc",
                "weight": "abc",
                "volume": "abc"
            },
        ],
        "member": "yes",
        "gender": "male",
        "age": "abc",
        "total_price": "abc"
    }

    # Expected changes in allItem
    allItemsExpected["item_id"].append("abc")
    allItemsExpected["name"].append("abc")
    allItemsExpected["price"].append(None)
    allItemsExpected["category"].append("abc")
    allItemsExpected["item_weight"].append(None)
    allItemsExpected["item_volume"].append(None)
    allItemsExpected["sold"].append(1)
    allItemsExpected["member"].append(1)
    allItemsExpected["walk_in"].append(0)
    allItemsExpected["male"].append(1)
    allItemsExpected["female"].append(0)
    allItemsExpected["age"].append(0)
    allItemsExpected["pay_cash"].append(1)
    allItemsExpected["pay_card"].append(0)
    allItemsExpected["pay_ewallet"].append(0)

    for shopNumber in range(1, 46):
        shopName = "Shop " + str(shopNumber)
        allItemsExpected[shopName].append(0)

    processTransaction(transaction)
    assert allItems == allItemsExpected, "Test for text value instead of numerical value failed."

    # Testing numerical value instead of text value
    transaction = {
        "_id": ObjectId('636625f0c599615f2253d0e7'),
        "shop_name": 123,
        "date_time": 123,
        "pay_method": 123,
        "item": [
            {
                "item_id": 123,
                "name": 123,
                "category": 123,
                "price": 123,
                "weight": 123,
                "volume": 123
            },
        ],
        "member": "yes",
        "gender": 123,
        "age": 123,
        "total_price": 123
    }

    # Expected changes in allItem
    allItemsExpected["item_id"].append(123)
    allItemsExpected["name"].append(None)
    allItemsExpected["price"].append(123)
    allItemsExpected["category"].append(None)
    allItemsExpected["item_weight"].append(123)
    allItemsExpected["item_volume"].append(123)
    allItemsExpected["sold"].append(1)
    allItemsExpected["member"].append(1)
    allItemsExpected["walk_in"].append(0)
    allItemsExpected["male"].append(0)
    allItemsExpected["female"].append(0)
    allItemsExpected["age"].append(123)
    allItemsExpected["pay_cash"].append(0)
    allItemsExpected["pay_card"].append(0)
    allItemsExpected["pay_ewallet"].append(0)

    for shopNumber in range(1, 46):
        shopName = "Shop " + str(shopNumber)
        allItemsExpected[shopName].append(0)

    processTransaction(transaction)
    assert allItems == allItemsExpected, "Test for numerical value instead of text value failed."

    print("Everything passed.")


