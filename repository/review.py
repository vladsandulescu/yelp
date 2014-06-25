__author__ = 'Vlad'


class Review:
    def __init__(self, friends, reviews, date, text, rating):
        self.friends = friends
        self.reviews = reviews
        self.date = date
        self.text = text
        self.rating = rating

    def encode(self):
        return \
            {
                "friends": self.friends,
                "reviews": self.reviews,
                "date": self.date,
                "text": self.text,
                "rating": self.getRating(self.rating)
            }

    @staticmethod
    def getRating(html):
        return html[10:13]
