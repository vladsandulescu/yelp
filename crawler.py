import json
import sys

from pymongo import MongoClient
from bson import ObjectId

from importio import importio
from importio import latch
from repository.business import Business
from repository.review import Review
from repository.user import User
from settings.settings import Settings


class Crawler:
    def __init__(self, database, collection, connector, client):
        self.database = database
        self.collection = collection
        self.connector = connector
        self.client = client
        self.dataRows = []
        self.queryLatch = latch.latch(0)

    def callback(self, query, message):
        # Disconnect messages happen if we disconnect the client library while a query is in progress
        if message["type"] == "DISCONNECT":
            print "Query in progress when library disconnected"

        if message["type"] == "MESSAGE":
            if "errorType" in message["data"]:

                # In this case, we received a message, but it was an error from the external service
                print "Got an error!"
                print json.dumps(message["data"], indent=4)
            else:
                self.dataRows.extend(message["data"]["results"])

        # When the query is finished, countdown the latch so the program can continue when everything is done
        if query.finished():
            self.queryLatch.countdown()

    def crawl(self, query_url):
        self.dataRows = []
        self.queryLatch = latch.latch(1)
        self.client.query(
            {"connectorGuids": self.connector,
             "input": {"webpage/url": query_url}
            }, self.callback)

        self.queryLatch.await()

    def update_user_friends(self, business):
        try:
            firstUsersThreshold = 20
            business.activeReviews.sort()
            for review in business.activeReviews[:firstUsersThreshold]:
                if review.user.friends > 0 and len(review.user.friends_list) == 0:
                    review.user.friends_list = self.crawl_friends(review.user.link, review.user.friends)

                self.save_business(business)
        except:
            print "Unexpected error:", sys.exc_info()[0]

    def crawl_friends(self, url, friends_count):
        friends = []
        page = 100
        print "Crawl friends for " + url + " - " + str(friends_count)
        for i in xrange(0, int(friends_count) / page + 1, 1):
            crawl_url = str(url).replace('user_details', 'user_details_friends') + '&start=' + str(i * page)
            self.crawl(crawl_url)
            try:
                friends.extend([User(row["user"], row["user/_text"], row["friends"], row["reviews"],
                                     row["location"] if "location" in row else "")
                                for row in self.dataRows])
            except:
                print "Unexpected error:", sys.exc_info()[0]

        return friends

    def save(self, business, is_not_recommended):
        for row in self.dataRows:
            if "text" in row:
                user = User(row["user"], row["user/_text"] if "user/_text" in row else row["user"],
                            row["friends"], row["reviews"],
                            row["location"] if "location" in row else "", [])
                review = Review(row["friends"], row["reviews"], row["date"], row["text"], row["rating"], user)
                if is_not_recommended:
                    business.filteredReviews.append(review)
                else:
                    business.activeReviews.append(review)

        self.save_business(business)

    def save_business(self, business):
        business._id = ObjectId()
        existing = self.database[self.collection].find_one({"name": business.name})
        if existing is not None:
            business._id = existing["_id"]
        self.database[self.collection].save(business.encode())

    @staticmethod
    def exists(name, database, collection):
        return database[collection].find_one({"name": name}) is not None

    @staticmethod
    def generate_urls(businesses_file, filtered_urls_file):

        businesses_file = open(businesses_file)
        filtered_urls_file = open(filtered_urls_file)

        businesses = [line.strip() for line in businesses_file]
        filtered_urls = json.load(filtered_urls_file)

        biz_data = []
        for business in businesses:
            items = business.split(",")
            biz_data.append(Business(name=items[0].decode('utf-8'), activeBaseUrl=items[2], activeCount=items[3]))

        businesses_data = []
        for value in filtered_urls["data"]:
            name = value["name"][0]
            business = [x for x in biz_data if x.name == name]
            if len(business) == 1:
                business = business[0]
                businesses_data.append(
                    Business(name=name,
                             filteredBaseUrl=value["_pageUrl"],
                             filteredCount=value["filtered"][0],
                             activeBaseUrl=business.activeBaseUrl,
                             activeCount=int(business.activeCount)))

        businesses_file.close()
        filtered_urls_file.close()

        for business in businesses_data:
            for i in xrange(0, business.activeCount / 40 + 1, 1):
                business.activeUrls.append(business.activeBaseUrl + "?start=" + str(i * 40))
            for i in xrange(0, business.filteredCount / 10 + 1, 1):
                business.filteredUrls.append(business.filteredBaseUrl + "?not_recommended_start=" + str(i * 10))

        return businesses_data

    @staticmethod
    def run(businesses_file, filtered_urls_file, collection):

        db = MongoClient(Settings.CONNECTION_STRING)[Settings.DATABASE_NAME]
        client = importio.importio(
            user_id=Settings.IMPORTIO_USER_ID,
            api_key=Settings.IMPORTIO_API_KEY,
            host=Settings.IMPORTIO_HOST)
        client.connect()

        crawler_active = Crawler(db, collection, Settings.IMPORTIO_CRAWLER_ACTIVE, client)
        crawler_filtered = Crawler(db, collection, Settings.IMPORTIO_CRAWLER_FILTERED, client)
        crawler_friends = Crawler(db, collection, Settings.IMPORTIO_CRAWLER_FRIENDS, client)

        businesses = Crawler.generate_urls(businesses_file, filtered_urls_file)
        done = 0
        total = len(businesses)
        print 'Done {0}/{1}'.format(done, total)

        for business in businesses:
            if not Crawler.exists(business.name, db, collection):
                for url in business.activeUrls:
                    crawler_active.crawl(url)
                    crawler_active.save(business, is_not_recommended=False)
                for url in business.filteredUrls:
                    crawler_filtered.crawl(url)
                    crawler_filtered.save(business, is_not_recommended=True)
            crawler_friends.update_user_friends(business)
            client.disconnect()
            client.connect()
            print 'Client reconnected'

            done += 1
            print 'Done {0}/{1}'.format(done, total)

        return True