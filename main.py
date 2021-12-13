# Anshul Anugu
# Final Project - Housing Project
# Purpose: Read in three files, clean data, read into SQL database, and finally display the data to user.
# Catch exceptions
import pandas as pd
import numpy as np
import re
import pymysql.cursors
from files import *

GuidList = []

# read in records into three data frames
housingDataFrame = pd.read_csv(housingFile)
incomeDataFrame = pd.read_csv(incomeFile)
zipDataFrame = pd.read_csv(zipFile)

# join data based on guid
HousingAndIncome = housingDataFrame.join(incomeDataFrame.set_index('guid'),
                                         on='guid',
                                         lsuffix='_housing',
                                         rsuffix='_income')
CombinedDataFrame = HousingAndIncome.join(zipDataFrame.set_index('guid'),
                                          on='guid')
CombinedDataFrame = CombinedDataFrame.drop(columns=['zip_code_housing', 'zip_code_income'])
CombinedDataFrame = CombinedDataFrame.rename(columns={'housing_median_age': 'median_age'})

# stripping hyphens from guid so that it will properly read into the database
for guid, row in CombinedDataFrame.iterrows():
    stripped = row['guid'].replace('-', "")
    GuidList.append(stripped)

GuidDf = pd.DataFrame(data=GuidList, columns=['guid'])
CombinedDataFrame.update(GuidDf)

# cleaning data
# dropping corrupt guid
for guid, row in CombinedDataFrame.iterrows():
    check = re.fullmatch("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['guid'])

    if check is not None:
        CorruptGuid = row['guid']
        CombinedDataFrame.replace(CorruptGuid, np.nan, True)
        CombinedDataFrame.dropna(inplace=True)

    else:
        pass

# assigning corrupt zip code with the beginning number followed by 0000 of good zip code of the same state
for code, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['zip_code'])

    if check is not None:
        CorruptRecord = row['guid']
        CorruptZip = row['zip_code']
        City = row['city']
        State = row['state']

        for CityState, record in CombinedDataFrame.iterrows():
            goodCheck = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", record['zip_code'])
            if goodCheck is None:
                if CorruptRecord != record['guid']:
                    if State == record['state']:
                        UseZip = record['zip_code']
                        res = [int(x) for x in str(UseZip)]
                        first = res[0]
                        zipPadded = str(first) + "0000"
                        int(zipPadded)
                    else:
                        pass
                else:
                    pass
        CombinedDataFrame.replace(CorruptZip, zipPadded, True)
    else:
        pass

# replace corrupt age, rooms, bedrooms, population, households, value, & income with random number between certain range
for age, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['median_age'])

    if check is not None:
        CorruptAge = row['median_age']
        randAge = np.random.randint(10, 50)
        CombinedDataFrame.replace(CorruptAge, randAge, True)
    else:
        pass


for rooms, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['total_rooms'])

    if check is not None:
        CorruptRooms = row['total_rooms']
        randRoom = np.random.randint(1000, 2000)
        CombinedDataFrame.replace(CorruptRooms, randRoom, True)
    else:
        pass

for bedrooms, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['total_bedrooms'])

    if check is not None:
        CorruptBedroom = row['total_bedrooms']
        randBedroom = np.random.randint(1000, 2000)
        CombinedDataFrame.replace(CorruptBedroom, randBedroom, True)
    else:
        pass


for population, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['population'])

    if check is not None:
        CorruptPopulation = row['population']
        randPopulation = np.random.randint(5000, 10000)
        CombinedDataFrame.replace(CorruptPopulation, randPopulation, True)
    else:
        pass

for households, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['households'])

    if check is not None:
        CorruptHouseholds = row['households']
        randHouseholds = np.random.randint(500, 2500)
        CombinedDataFrame.replace(CorruptHouseholds, randHouseholds, True)
    else:
        pass

for value, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['median_house_value'])

    if check is not None:
        CorruptValue = row['median_house_value']
        randValue = np.random.randint(100000, 250000)
        CombinedDataFrame.replace(CorruptValue, randValue, True)
    else:
        pass

for income, row in CombinedDataFrame.iterrows():
    check = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['median_income'])

    if check is not None:
        CorruptIncome = row['median_income']
        randIncome = np.random.randint(100000, 750000)
        CombinedDataFrame.replace(CorruptIncome, randIncome, True)
    else:
        pass

# connecting to SQL to access housing_project database
try:
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()
    exit()
# Printing output for the verification of import
print("Beginning import")

# Insert DataFrame records one by one
cursor = myConnection.cursor()
ColumnList = "`,`".join([str(i) for i in CombinedDataFrame.columns.tolist()])

for i, row in CombinedDataFrame.iterrows():
    sql = "INSERT INTO `housing` (`" + ColumnList + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    myConnection.commit()

# Collecting total records imported into the database.
# Because I joined the three files, there is only one cleaning process after which files are then imported.

sqlCount = "SELECT COUNT(*) FROM `housing`"
cursor.execute(sqlCount)
CountResult = cursor.fetchall()
for result in CountResult:
    TotalRecordsImported = result['COUNT(*)']

# Printing output for the verification of cleaned import
print("Cleaning Housing File data")
print(f"{TotalRecordsImported} records imported into the database")
print("Cleaning Income File data")
print(f"{TotalRecordsImported} records imported into the database")
print("Cleaning ZIP File data")
print(f"{TotalRecordsImported} records imported into the database")
print("Import completed")
print()

# Create variables to hold input. Using input to give correct output based on records in database.

print("Beginning validation")
print()

Valid = False
while Valid is False:
    askForRooms = input("Total Rooms: {Enter any integer to know more about rooms} ")
    try:
        if str.isnumeric(askForRooms):
            if int(askForRooms) >= 0:
                sqlBedrooms = "SELECT FORMAT(SUM(total_bedrooms),0) sum_bedrooms FROM housing WHERE total_rooms > %s"
                cursor.execute(sqlBedrooms, askForRooms)
                BedroomsResult = cursor.fetchall()
                for result in BedroomsResult:
                    TotalBedroomsResult = result['sum_bedrooms']
                print(f"Total Rooms: {askForRooms}")
                print(f"For locations with more than {askForRooms} rooms, there are a total of {TotalBedroomsResult} bedrooms.")
                print()
                Valid = True
        elif int(askForRooms) < 0:
            print("Please enter only positive integers")
            Valid = False
        else:
            print("Only integers.")
            Valid = False
    except Exception as e:
        print(f"Please try again.")
        print(f"\t{e}")

Question = False
while Question is False:
    askForZipCode = input("ZIP Code: {Enter any five digit integer to know more about that zip code} ")
    try:
        if str.isnumeric(askForZipCode):
            if int(askForZipCode) >= 0:
                if len(askForZipCode) == 5:
                    sqlZipCode = "SELECT FORMAT(ROUND(AVG(median_income)),0) avg_income FROM housing WHERE zip_code = %s"
                    cursor.execute(sqlZipCode, askForZipCode)
                    ZipCodeResult = cursor.fetchall()
                    for result in ZipCodeResult:
                        IncomeResult = result['avg_income']
                    print(f"ZIP Code: {askForZipCode}")
                    print(f"The median household income for ZIP code {askForZipCode} is {IncomeResult}.")
                    print()
                    Question = True
                else:
                    print("Zip Code must be five digits long.")
                    Question = False
        elif int(askForZipCode) < 0:
            print("Please enter only positive integers")
            Question = False
        else:
            print("Only integers.")
            Question = False
    except Exception as e:
        print(f"Please try again.")
        print(f"\t{e}")

# Closing sql
myConnection.close()

print("Program exiting.")