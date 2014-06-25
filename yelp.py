__author__ = 'Vlad'


# -*- coding: utf-8 -*-
import json
import sys
import urllib
import urllib2

import oauth2

from settings.settings import Settings


API_HOST = 'api.yelp.com'
SEARCH_LIMIT = 20
SEARCH_PATH = '/v2/search/'
BUSINESS_PATH = '/v2/business/'


def request(host, path, url_params=None):
    """Prepares OAuth authentication and sends the request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        urllib2.HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    encoded_params = urllib.urlencode(url_params)

    url = 'http://{0}{1}?{2}'.format(host, path, encoded_params)

    consumer = oauth2.Consumer(Settings.YELP_CONSUMER_KEY, Settings.YELP_CONSUMER_SECRET)
    oauth_request = oauth2.Request('GET', url, {})
    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': Settings.YELP_TOKEN,
            'oauth_consumer_key': Settings.YELP_CONSUMER_KEY
        }
    )
    token = oauth2.Token(Settings.YELP_TOKEN, Settings.YELP_TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()

    # print 'Querying {0} ...'.format(url)

    conn = urllib2.urlopen(signed_url, None)
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()

    return response


def search(term, location, limit, offset):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """
    url_params = {
        'term': term,
        'location': location,
        'limit': limit,
        'offset': offset
    }

    return request(API_HOST, SEARCH_PATH, url_params=url_params)


def query_api(output, term, location, limit, offset):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    response = search(term, location, limit, offset)

    businesses = response.get('businesses')

    if not businesses:
        print 'No businesses for {0} in {1} found.'.format(term, location)
        return

    for business in businesses:
        s = business["name"].replace(',', '-') \
            + "," + business["id"] \
            + "," + (business["url"].encode('utf-8')) \
            + "," + str(business["review_count"])
        output.write(s.encode('utf-8'))
        output.write("\n")


def run(output, term, location):
    with open('data/' + output + '.csv', 'w') as outputFile:
        for i in range(0, 1000, 1):
            try:
                query_api(outputFile, term, location, SEARCH_LIMIT, i * SEARCH_LIMIT)
            except urllib2.HTTPError as error:
                sys.exit('Encountered HTTP error {0}. Abort program.'.format(error.code))


def main():
    run('restaurants_la', 'restaurants', 'Los Angeles, CA')


if __name__ == '__main__':
    main()