__author__ = 'Vlad'

import sys

from crawler import Crawler


def run(collection):
    businesses_file = 'data/' + collection + '.csv'
    filtered_urls_file = 'data/filtered_urls_' + collection + '.json'

    retries = 0
    max_retries = 10
    result = False

    while result is False and retries < max_retries:
        try:
            result = Crawler.run(businesses_file, filtered_urls_file, collection)
        except:
            retries += 1
            print "Unexpected error:", sys.exc_info()[0]
            print "Will retry to run"
            if retries == max_retries:
                print "Unexpected error:", sys.exc_info()[0]
                print "Max retries number was reached. Will stop."
                raise


def main():
    run('restaurants_sf')


if __name__ == '__main__':
    main()