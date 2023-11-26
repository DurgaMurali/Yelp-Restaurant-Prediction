import pandas as pd
from sklearn.model_selection import train_test_split

def getUserLists():
    userItemDict = {}
    userRatingDict = {}
    with open('../UserItemList.txt') as userItemFile:
        userItems = userItemFile.readlines()
    userItemFile.close()

    with open('../UserRatingList.txt') as userRatingFile:
        userRatings = userRatingFile.readlines()
    userRatingFile.close()

    for line in userItems:
        userSplit = line.split(':')
        userItemDict[userSplit[0].strip()] = userSplit[1].strip()

    for line in userRatings:
        ratingSplit = line.split(':')
        userRatingDict[ratingSplit[0].strip()] = ratingSplit[1].strip()
    
    return (userItemDict, userRatingDict)


def getItemLists():
    itemUserDict = {}
    itemRatingDict = {}
    with open('../ItemUserList.txt') as itemUserFile:
        userItems = itemUserFile.readlines()
    itemUserFile.close()

    with open('../ItemRatingList.txt') as itemRatingFile:
        userRatings = itemRatingFile.readlines()
    itemRatingFile.close()

    for line in userItems:
        userSplit = line.split(':')
        itemUserDict[userSplit[0].strip()] = userSplit[1].strip()

    for line in userRatings:
        ratingSplit = line.split(':')
        itemRatingDict[ratingSplit[0].strip()] = ratingSplit[1].strip()
        
    return (itemUserDict, itemRatingDict)


def getFriendsList():
    friendsDF = pd.read_csv('../yelp-user-cleaned.csv')
    friendsDict = {}
    for index in range(len(friendsDF)):
        friendsDict[friendsDF.loc[index]['User']] = friendsDF.loc[index]['Friends']

    return friendsDict


def getRatingsList():
    ratingsDict = {}
    ratingsDict['1.0'] = 1
    ratingsDict['2.0'] = 2
    ratingsDict['3.0'] = 3
    ratingsDict['4.0'] = 4
    ratingsDict['5.0'] = 5
    return ratingsDict


def splitTrainAndTestData():
    data = pd.read_csv('../yelp-ratings-cleaned.csv')
    train_data, test_data = train_test_split(data, test_size=0.15, train_size=0.85, shuffle=True)
    train_data = train_data.reset_index(drop=True)
    test_data = test_data.reset_index(drop=True)
    return (train_data, test_data)