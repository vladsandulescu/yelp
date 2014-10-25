import calendar
import re
import datetime

__author__ = 'Vlad'


class Review:
    def __init__(self, friends, reviews, date, text, rating, user):
        self.friends = friends
        self.reviews = reviews
        self.date = self.getDate(date)
        self.text = text
        self.rating = self.getRating(rating)
        self.user = user

    def __cmp__(self, other):
        return cmp(self.date, other.date)

    def encode(self):
        return \
            {
                "friends": self.friends,
                "reviews": self.reviews,
                "date": self.date,
                "text": self.text,
                "rating": self.rating,
                "user": self.user.encode()
            }

    @staticmethod
    def getRating(html):
        return html[10:13]

    def getDate(self, date):
        if isinstance(date, list):
            match = re.search(r'(\d+/\d+/\d+)', date[0])
            extracted_date = match.group()
        else:
            extracted_date = date
        try:
            pretty_date = calendar.timegm(datetime.datetime.strptime(extracted_date, "%m/%d/%Y").timetuple())
        except:
            pretty_date = 0

        return pretty_date
