import itertools
from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import snscrape.modules.twitter as sntwitter
import datetime
from data_access import StorageService
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from text_summarizer import TextSummarizer


class BackgroundWorker:
    def __init__(self):
        executors = {
            'default': ThreadPoolExecutor(3),
            'processpool': ProcessPoolExecutor(3)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1
        }
        self.__scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, timezone=utc)
        self.__scheduler.add_job(self.__execute_twitter_scraper_worker, 'interval', seconds=60,
                                 id='twitter_scraper_worker')
        self.__scheduler.add_job(self.__execute_sentiment_analyser_worker, 'interval', seconds=20,
                                 id='sentiment_analyser_worker')
        self.__scheduler.add_job(self.__execute_account_sentiment_analyser_worker, 'interval', seconds=20,
                                 id='account_sentiment_analyser_worker')

    def start(self):
        self.__scheduler.start()

    def __execute_twitter_scraper_worker(self):
        storage = StorageService()
        accounts = [item['username'] for item in storage.getAccounts(None)]
        for account in accounts:
            try:
                q = f"from:{account}"
                until = datetime.datetime.strftime(datetime.date.today() + datetime.timedelta(days=1), '%Y-%m-%d')
                q += f" until:{until}"
                since = datetime.datetime.strftime(datetime.datetime(2023, 2, 1), '%Y-%m-%d')
                q += f" since:{since}"
                items = list(sntwitter.TwitterSearchScraper(q).get_items())

                items = list(filter(lambda x: x.inReplyToUser is None, items))
                for item in items:
                    try:
                        tws = list(sntwitter.TwitterTweetScraper(item.conversationId,
                                                                 mode=sntwitter.TwitterTweetScraperMode.SCROLL).get_items())

                        tweets = list(
                            map(lambda x: {'id': x.id, 'account': account, 'conversationId': x.conversationId,
                                           'content': x.renderedContent, 'time': x.date,
                                           'authorName': x.user.username,
                                           'authorId': x.user.id}, tws))

                        audiences = list(map(lambda aud: {'id': aud[0].split('@@')[1], 'account': account,
                                                          'name': aud[0].split('@@')[0],
                                                          'tweetIds': list(map(lambda s: s['id'], list(aud[1])))},
                                             [(k, list(g)) for k, g in itertools.groupby(tweets, lambda
                                                 x: f'{x["authorName"]}@@{x["authorId"]}')]))

                        # Insert or Update DB
                        tweetIds = list(map(lambda x: x['id'], storage.getTweets({'account': account}, {'id': 1})))
                        audienceIds = list(
                            map(lambda x: x['id'], storage.getAudiences({'account': account}, {'id': 1})))

                        tweetsShouldBeInsert = list(filter(lambda x: x['id'] not in tweetIds, tweets))
                        audiencesShouldBeInsert = list(filter(lambda x: x['id'] not in audienceIds, audiences))
                        audiencesShouldBeUpdate = list(filter(lambda x: x['id'] in audienceIds, audiences))

                        if len(tweetsShouldBeInsert) > 0:
                            storage.insertTweets(tweetsShouldBeInsert)
                        if len(audiencesShouldBeInsert) > 0:
                            storage.insertAudiences(audiencesShouldBeInsert)
                        if len(audiencesShouldBeUpdate) > 0:
                            storage.updateAudiences(audiencesShouldBeUpdate)

                    except Exception as ex:
                        print(ex)
            except Exception as e:
                print(e)

    def __execute_sentiment_analyser_worker(self):
        storage = StorageService()
        tweets = storage.getTweets({}, {})
        conversationTweets = [(k, list(g)) for k, g in itertools.groupby(tweets, lambda t: t['conversationId'])]
        for (key, group) in conversationTweets:
            try:
                conversationMessageCount = len(group)
                conversationNegatives = 0
                conversationPositive = 0
                conversationNeutral = 0

                audienceConversationSentiments = []
                audienceConversationTweets = [(k, list(g)) for k, g in itertools.groupby(group, lambda t: t['authorName'])]
                for (author, authorTweets) in audienceConversationTweets:
                    authorConversationMessageCount = len(authorTweets)
                    authorConversationNegatives = 0
                    authorConversationPositive = 0
                    authorConversationNeutral = 0

                    for tweet in authorTweets:
                        try:
                            score = SentimentIntensityAnalyzer().polarity_scores(tweet["content"])
                            neg = score['neg']
                            neu = score['neu']
                            pos = score['pos']

                            if neg > pos:
                                conversationNegatives += 1
                                authorConversationNegatives += 1
                            elif pos > neg:
                                conversationPositive += 1
                                authorConversationPositive += 1
                            elif pos == neg:
                                conversationNeutral += 1
                                authorConversationNeutral += 1
                        except Exception as e:
                            print(e)

                    negPercent = self.__percentage(authorConversationNegatives, authorConversationMessageCount)
                    posPercent = self.__percentage(authorConversationPositive, authorConversationMessageCount)
                    neuPercent = self.__percentage(authorConversationNeutral, authorConversationMessageCount)
                    audienceConversationSentiments.append(
                        {'audience': author, 'total': authorConversationMessageCount, 'negative': authorConversationNegatives,
                         'positive': authorConversationPositive, 'neutral': authorConversationNeutral,
                         'negPercent': negPercent, 'posPercent': posPercent, 'neuPercent': neuPercent})

                negPercent = self.__percentage(conversationNegatives, conversationMessageCount)
                posPercent = self.__percentage(conversationPositive, conversationMessageCount)
                neuPercent = self.__percentage(conversationNeutral, conversationMessageCount)

                accountSentiments = storage.getSentiments({'account': group[0]['account']}, None)
                exists = next((s for s in accountSentiments if s['conversation']['id'] == key), None)
                if exists is None:
                    storage.insertSentiment(
                        {'account': group[0]['account'], 'conversation':
                            {'id': key, 'tweet': group[0]['content'], 'tweetId': group[0]['id'],
                             'total': conversationMessageCount, 'negative': conversationNegatives,
                             'positive': conversationPositive, 'neutral': conversationNeutral,
                             'negPercent': negPercent, 'posPercent': posPercent, 'neuPercent': neuPercent,
                             'audiences': audienceConversationSentiments}})
                else:
                    conversation = exists['conversation']
                    conversation['total'] = conversationMessageCount
                    conversation['negative'] = conversationNegatives
                    conversation['positive'] = conversationPositive
                    conversation['neutral'] = conversationNeutral
                    conversation['negPercent'] = negPercent
                    conversation['posPercent'] = posPercent
                    conversation['neuPercent'] = neuPercent
                    storage.updateSentiment(exists)
            except Exception as ex:
                print(ex)

    def __execute_account_sentiment_analyser_worker(self):
        try:
            storage = StorageService()
            tweets = storage.getTweets({}, {})
            accountTweets = [(k, list(g)) for k, g in itertools.groupby(tweets, lambda t: t['account'])]
            selfAccountTweets = [(acc, [t for t in tweets if t['authorName'] == acc]) for (acc, tweets) in accountTweets]
            for (account, selfTweets) in selfAccountTweets:
                contentShouldBeSummarize = ''
                for selfTweet in selfTweets:
                    contentShouldBeSummarize += f'{selfTweet["content"]}\r'

                text_summarizer = TextSummarizer()
                summary = text_summarizer.get_summary(contentShouldBeSummarize)
                storage.updateAccount(account, summary=summary)

            for (account, tweet) in accountTweets:
                sentiments = storage.getSentiments({'account': account}, None)
                totalAccountTweets = sum([s['conversation']['total'] for s in sentiments])
                positiveAccountTweets = sum([s['conversation']['positive'] for s in sentiments])
                negativeAccountTweets = sum([s['conversation']['negative'] for s in sentiments])
                neutralAccountTweets = sum([s['conversation']['neutral'] for s in sentiments])
                negativeAccountTweetsPercent = self.__percentage(negativeAccountTweets, totalAccountTweets)
                positiveAccountTweetsPercent = self.__percentage(positiveAccountTweets, totalAccountTweets)
                neutralAccountTweetsPercent = self.__percentage(neutralAccountTweets, totalAccountTweets)
                storage.updateAccount(account, sentiment={'totalAccountTweets': totalAccountTweets,
                                                          'positiveAccountTweets': positiveAccountTweets,
                                                          'negativeAccountTweets': negativeAccountTweets,
                                                          'neutralAccountTweets': neutralAccountTweets,
                                                          'negativeAccountTweetsPercent': negativeAccountTweetsPercent,
                                                          'positiveAccountTweetsPercent': positiveAccountTweetsPercent,
                                                          'neutralAccountTweetsPercent': neutralAccountTweetsPercent})


        except Exception as ex:
            print(ex)

    def __percentage(self, part, total):
        return 100 * float(part) / float(total)
