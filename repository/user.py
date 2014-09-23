__author__ = 'Vlad'


class User:
    def __init__(self, link, name, friends, reviews, location, friends_list=[]):
        self.link = link
        self.name = name
        self.friends = friends
        self.reviews = reviews
        self.location = location
        self.friends_list = friends_list

    def encode(self):
        return \
            {
                "link": self.link,
                "name": self.name,
                "friends": self.friends,
                "location": self.location,
                "reviews": self.reviews,
                "friends_list": list(friend.encode() for friend in self.friends_list)
            }