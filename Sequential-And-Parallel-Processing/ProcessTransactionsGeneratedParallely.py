import datetime
import pymongo
import pandas
from concurrent.futures import ThreadPoolExecutor


# Function to process transactions in the specified shop at the specified hour and return the result
def processTransactionsAndReturn(shopNameAndTime):

    (shopName, year, month, day, hour) = shopNameAndTime

    # The master record for this hour and shop
    allItems = {
        "item_id": [],
        "name": [],
        "price": [],
        "category": [],
        "item_weight": [],
        "item_volume": [],
        "sold": [],
        "sold_amount": [],
        "member": [],
        "walk_in": [],
        "male": [],
        "female": [],
        "age": []
    }

    startOfHour = datetime.datetime(year, month, day, hour, 0, 0)
    endOfHour = datetime.datetime(year, month, day, hour, 59, 59)

    connectionString = "mongodb+srv://User1:user1@cluster0.z5ffg.mongodb.net/test"
    with pymongo.MongoClient(connectionString) as client:
        database = client["MyShop"]
        transactionCollection = database["Transaction"]

        for transaction in transactionCollection.find(
                {"$and": [{"date_time": {"$gte": startOfHour}},  # gte: greater than or equal to
                          {"date_time": {"$lte": endOfHour}},  # lte: less than or equal to
                          {"shop_name": str(shopName)}]}):

            # Line 43-147 reused script in ProcessTransactionsGeneratedSequentially.py
            # Skip if item is missing
            if "item" not in transaction.keys():
                continue

            # Set isMember flag
            isMember = False
            if "member" in transaction.keys():
                member = transaction["member"]
                if type(member) == str:
                    member = member.strip()
                    member = member.lower()
                    if member == "yes":
                        isMember = True

            # Set isMale or isFemale flag
            isMale = False
            isFemale = False
            if "gender" in transaction.keys():
                gender = transaction["gender"]
                if type(gender) == str:
                    gender = gender.strip()
                    gender = gender.lower()
                    if gender == "male":
                        isMale = True
                    elif gender == "female":
                        isFemale = True

            # Set age value
            if "age" in transaction.keys():
                age = transaction["age"]
                if type(age) == int and age >= 18 and age <= 150:
                    pass
                else:
                    age = 0
            else:
                age = 0

            for item in transaction["item"]:

                # Skip if item id is missing
                if "item_id" not in item:
                    continue

                item_id = item["item_id"]
                if item_id not in allItems["item_id"]:  # New item
                    allItems["item_id"].append(item_id)
                    allItems["name"].append(None)
                    allItems["price"].append(None)
                    allItems["category"].append(None)
                    allItems["item_weight"].append(None)
                    allItems["item_volume"].append(None)
                    allItems["sold"].append(0)
                    allItems["sold_amount"].append(0)
                    allItems["member"].append(0)
                    allItems["walk_in"].append(0)
                    allItems["male"].append(0)
                    allItems["female"].append(0)
                    allItems["age"].append(0)
                item_index = allItems["item_id"].index(item_id)

                # Basic information of the item
                if allItems["name"][item_index] is None and "name" in item.keys():
                    allItems["name"][item_index] = item["name"]

                if allItems["price"][item_index] is None and "price" in item.keys():
                    allItems["price"][item_index] = item["price"]

                if allItems["category"][item_index] is None and "category" in item.keys():
                    allItems["category"][item_index] = item["category"]

                if allItems["item_weight"][item_index] is None and "weight" in item.keys():
                    allItems["item_weight"][item_index] = item["weight"]

                if allItems["item_volume"][item_index] is None and "volume" in item.keys():
                    allItems["item_volume"][item_index] = item["volume"]

                # Increment the total number of items sold and update the total sales amount and
                # the total weight or volume sold
                allItems["sold"][item_index] = allItems["sold"][item_index] + 1

                if allItems["price"][item_index] is not None:
                    allItems["sold_amount"][item_index] = allItems["sold_amount"][item_index] + allItems["price"][
                        item_index]

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

    return allItems


# Function to parallely process the transaction records of the specified shop name on the specified day
def processTransactionsParallely(year, month, day, numberOfThreads):

    # The master record for the whole day
    global allItemsMaster
    allItemsMaster = {
        "item_id": [],
        "name": [],
        "price": [],
        "category": [],
        "item_weight": [],
        "item_volume": [],
        "sold": [],
        "sold_amount": [],
        "member": [],
        "walk_in": [],
        "male": [],
        "female": [],
        "age": []
    }

    # List of all shop names and all hours in the day
    listOfShopNameAndTime = []
    for shopNumber in range(1, 46):
        shopName = "Shop " + str(shopNumber)
        for hour in range(0, 24):
            listOfShopNameAndTime.append((shopName, year, month, day, hour))

    with ThreadPoolExecutor(numberOfThreads) as executor:
        listOfResults = executor.map(processTransactionsAndReturn, listOfShopNameAndTime)

    # Combine the results of each shop and hour to the master record
    for allItemsOfThisHourAndShop in listOfResults:
        item_index = 0
        for item_id in allItemsOfThisHourAndShop["item_id"]:
            if item_id not in allItemsMaster["item_id"]:  # New item
                allItemsMaster["item_id"].append(item_id)
                allItemsMaster["name"].append(None)
                allItemsMaster["price"].append(None)
                allItemsMaster["category"].append(None)
                allItemsMaster["item_weight"].append(None)
                allItemsMaster["item_volume"].append(None)
                allItemsMaster["sold"].append(0)
                allItemsMaster["sold_amount"].append(0)
                allItemsMaster["member"].append(0)
                allItemsMaster["walk_in"].append(0)
                allItemsMaster["male"].append(0)
                allItemsMaster["female"].append(0)
                allItemsMaster["age"].append(0)
            item_index_master = allItemsMaster["item_id"].index(item_id)

            # Basic information of item
            if allItemsMaster["name"][item_index_master] is None and allItemsOfThisHourAndShop["name"][item_index] is not None:
                allItemsMaster["name"][item_index_master] = allItemsOfThisHourAndShop["name"][item_index]

            if allItemsMaster["price"][item_index_master] is None and allItemsOfThisHourAndShop["price"][item_index] is not None:
                allItemsMaster["price"][item_index_master] = allItemsOfThisHourAndShop["price"][item_index]

            if allItemsMaster["category"][item_index_master] is None and allItemsOfThisHourAndShop["category"][
                item_index] is not None:
                allItemsMaster["category"][item_index_master] = allItemsOfThisHourAndShop["category"][item_index]

            if allItemsMaster["item_weight"][item_index_master] is None and allItemsOfThisHourAndShop["item_weight"][
                item_index] is not None:
                allItemsMaster["item_weight"][item_index_master] = allItemsOfThisHourAndShop["item_weight"][item_index]

            if allItemsMaster["item_volume"][item_index_master] is None and allItemsOfThisHourAndShop["item_volume"][
                item_index] is not None:
                allItemsMaster["item_volume"][item_index_master] = allItemsOfThisHourAndShop["item_volume"][item_index]

            # Sales information of item
            allItemsMaster["sold"][item_index_master] = allItemsMaster["sold"][item_index_master] + allItemsOfThisHourAndShop["sold"][
                item_index]
            allItemsMaster["sold_amount"][item_index_master] = allItemsMaster["sold_amount"][item_index_master] + \
                                                     allItemsOfThisHourAndShop["sold_amount"][item_index]

            # Information of people buying the item
            allItemsMaster["member"][item_index_master] = allItemsMaster["member"][item_index_master] + \
                                                allItemsOfThisHourAndShop["member"][
                                                    item_index]
            allItemsMaster["walk_in"][item_index_master] = allItemsMaster["walk_in"][item_index_master] + \
                                                 allItemsOfThisHourAndShop["walk_in"][
                                                     item_index]
            allItemsMaster["male"][item_index_master] = allItemsMaster["male"][item_index_master] + allItemsOfThisHourAndShop["male"][
                item_index]
            allItemsMaster["female"][item_index_master] = allItemsMaster["female"][item_index_master] + \
                                                allItemsOfThisHourAndShop["female"][
                                                    item_index]

            averageAgeMaster = allItemsMaster["age"][item_index_master]
            averageAge_thisHourAndShop = allItemsOfThisHourAndShop["age"][item_index]
            if averageAgeMaster != 0 and averageAge_thisHourAndShop != 0:
                allItemsMaster["age"][item_index_master] = (averageAgeMaster + averageAge_thisHourAndShop) / 2
            elif averageAge_thisHourAndShop != 0:
                allItemsMaster["age"][item_index_master] = averageAge_thisHourAndShop

            item_index = item_index + 1

    # Write the combined result to CSV file
    allItemsDataFrame = pandas.DataFrame(allItemsMaster)
    date = datetime.datetime(year, month, day)
    allItemsFileName = "Items " + date.strftime("%d %b %Y") + ".csv"
    allItemsFilePath = "C:/Users/user/Desktop/Big-Data-Programming-Project/Grocery-Data-Project/Sequential-And-Parallel-Processing/Dummy analysis results/" + allItemsFileName
    allItemsDataFrame.to_csv(allItemsFilePath)


