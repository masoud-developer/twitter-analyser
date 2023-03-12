from pymongo import MongoClient, UpdateOne


class StorageService:
    def __init__(self):
        self.__client = MongoClient('mongodb://masoud:NNNqwe123asd321MMM@mongo:27017')
        self.__db = self.__client['twitter']
        self.__accounts = self.__db['accounts']
        self.__conversationThreads = self.__db['conversationThreads']
        self.__audiences = self.__db['audiences']
        self.__sentiments = self.__db['sentiments']

    def insertTweets(self, tweets) -> int:
        result = self.__conversationThreads.insert_many(tweets)
        return len(result.inserted_ids)

    def getAccounts(self, fields: dict) -> list:
        result = list(self.__accounts.find({}, fields or {}))
        return result

    def getTweets(self, filter: dict, fields: dict) -> list:
        if fields is None:
            result = list(self.__conversationThreads.find(filter))
        else:
            result = list(self.__conversationThreads.find(filter, fields))
        return result

    def insertAudiences(self, audiences) -> int:
        result = self.__audiences.insert_many(audiences)
        return len(result.inserted_ids)

    def updateAudiences(self, audiences) -> int:
        updatesQuery = [UpdateOne({'id': x['id']}, {'$addToSet': {'tweetIds': {'$each': x['tweetIds']}}})
                        for i, x in enumerate(audiences)]
        result = self.__audiences.bulk_write(updatesQuery)
        return result.modified_count

    def getAudiences(self, filter: dict, fields: dict) -> list:
        if fields is None:
            result = list(self.__audiences.find(filter))
        else:
            result = list(self.__audiences.find(filter, fields))
        return result

    def getSentiments(self, filter: dict, fields: dict) -> list:
        result = list(self.__sentiments.find(filter, fields or {}))
        return result

    def insertSentiment(self, sentiment) -> int:
        result = self.__sentiments.insert_one(sentiment)
        return 1

    def updateSentiment(self, sentiment) -> int:
        result = self.__sentiments.replace_one({'_id': sentiment['_id']}, sentiment)
        return 1

    def updateAccount(self, account_name, summary=None, sentiment=None) -> int:
        update = {}
        if summary:
            update['summary'] = summary
        if sentiment:
            update['sentiment'] = sentiment
        result = self.__accounts.update_one({'username': account_name}, {'$set': update})
        return result.modified_count
