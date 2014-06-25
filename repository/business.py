__author__ = 'Vlad'


class Business:
    def __init__(self, name, activeBaseUrl=None, filteredBaseUrl=None, activeCount=None, filteredCount=None):
        self.name = name
        self.activeBaseUrl = activeBaseUrl
        self.filteredBaseUrl = filteredBaseUrl
        self.activeCount = activeCount
        self.filteredCount = filteredCount

        self._id = None
        self.activeUrls = []
        self.filteredUrls = []
        self.activeReviews = []
        self.filteredReviews = []

    def __eq__(self, other):
        return self.name == other.name

    def encode(self):
        return \
            {
                "_id": self._id,
                "name": self.name,
                "activeBaseUrl": self.activeBaseUrl,
                "filteredBaseUrl": self.filteredBaseUrl,
                "activeCount": self.activeCount,
                "filteredCount": self.filteredCount,
                "activeUrls": self.activeUrls,
                "filteredUrls": self.filteredUrls,
                "activeReviews": list(review.encode() for review in self.activeReviews),
                "filteredReviews": list(review.encode() for review in self.filteredReviews)
            }