from flask import request
from flask_restful import Resource
from data_access import StorageService


class AppController(Resource):
    def __init__(self):
        self.storage = StorageService()

    def accounts(self):
        return self.storage.getAccounts(fields={'_id': 0})

    def tweets(self, account):
        return self.storage.getTweets(filter={'account': account}, fields={'_id': 0, 'time': 0})

    def audiences(self, account):
        return self.storage.getAudiences(filter={'account': account}, fields={'_id': 0})[1:]

    def sentiments(self, account):
        return self.storage.getSentiments(filter={'account': account}, fields={'_id': 0})

    def get(self, **args):
        url = request.url
        if "tweets" in url:
            return self.tweets(args['account'])
        elif "accounts" in url:
            return self.accounts()
        elif "audience" in url:
            return self.audiences(args['account'])
        elif "sentiment" in url:
            return self.sentiments(args['account'])

    def post(self, body):
        return None