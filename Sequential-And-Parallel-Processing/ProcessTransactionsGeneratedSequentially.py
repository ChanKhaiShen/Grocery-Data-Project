import pymongo
import datetime
import pandas


# Function to process one transaction
def processTransaction(transaction):
    # Skip if item is missing
    if "item" not in transaction.keys():
        return

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
            allItems["sold_amount"][item_index] = allItems["sold_amount"][item_index] + allItems["price"][item_index]

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
        "sold_amount": [],
        "member": [],
        "walk_in": [],
        "male": [],
        "female": [],
        "age": []
    }

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


