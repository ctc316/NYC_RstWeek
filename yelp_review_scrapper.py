import sys
import requests
import time

from bs4 import BeautifulSoup
import json
import pandas as pd


def scrape_content(content):
    user_content = content.find('div', class_='media-story')
    username = user_content.find('a', class_='user-display-name')
    username = username.get_text().strip() if username else "_ghost"
    user_loc = user_content.find('li', class_='user-location')
    user_loc = user_loc.find('b').get_text().strip() if username else ""
    user_friends = user_content.find('li', class_='friend-count')
    user_friends = user_friends.find('b').get_text().strip() if user_friends else 0
    user_reviews = user_content.find('li', class_='review-count')
    user_reviews = user_reviews.find('b').get_text().strip() if user_reviews else 0
    user_photos = user_content.find('li', class_='photo-count')
    user_photos = user_photos.find('b').get_text().strip() if user_photos else 0
    
    review_content = content.find('div', class_='review-wrapper')
    rating = review_content.find('div', class_='i-stars')['title'].split(" ")[0].strip()
    time_created = review_content.find('span', class_='rating-qualifier').get_text().replace('\n', '').strip().split(' ')[0]
    text = review_content.find('p').get_text().strip()
    useful = review_content.find('a', class_='useful')
    if useful:
        useful = useful.find('span', class_='count').get_text().strip()
    useful = useful if useful else 0
    funny = review_content.find('a', class_='funny')
    if funny:
        funny = funny.find('span', class_='count').get_text().strip()
    funny = funny if funny else 0
    cool = review_content.find('a', class_='cool')
    if cool:
        cool = cool.find('span', class_='count').get_text().strip()
    cool = cool if cool else 0
    
    return [rating, time_created, text, useful, funny, cool, username, user_loc, user_friends, user_reviews, user_photos]
    

def get_proxies():
    url = 'https://www.us-proxy.org/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.find('tbody').find_all('tr')
    proxies = set()
    for row in rows:
        cols = row.find_all('td')
        # if cols[4].get_text() != 'elite proxy':
        #     continue
        proxies.add(cols[0].get_text() + ":" + cols[1].get_text())

    return proxies


def run_scrapper(start, end):
    yelp_rsts = pd.read_csv("data/yelp_rst_2019.csv")

    PER_PAGE = 20

    proxies = set()

    for i in range(yelp_rsts.shape[0])[max(0, start): min(yelp_rsts.shape[0], end)]:
        hasNextPage = True
        start = 0
        url = yelp_rsts.loc[i]['url'].split("?")[0] + "?sort_by=date_desc&start="
        print("=========================", i, yelp_rsts.loc[i]['alias'], "=======================")
        results = pd.DataFrame(data=[], columns=['rating', 'time_created', 'text', 'useful', 'funny', 'cool', 'username', 'user_loc', 'user_friends', 'user_reviews', 'user_photos'])

        while hasNextPage:
            page_url = url + str(start)
            print(page_url)
            success = False
            fails = set()
            if not proxies:
                proxies = get_proxies()
                print("New proxies:", len(proxies))
            
            success_proxy = ""
            for proxy in proxies:
                try:
                    # req = requests.get(page_url, timeout=30)
                    req = requests.get(page_url, proxies={"http": proxy, "https": proxy}, timeout=10)
                    if req.status_code >= 300:
                        raise Exception(req.status_code)
                    else:
                        success_proxy = proxy
                        break
                except:
                    fails.add(proxy)
                    print("Fail proxy:", proxy, ", remains:", len(proxies) - len(fails))
                    

            print("Fail proxies:", len(fails))
            proxies -= fails
            if not success_proxy:    
                continue

            soup = BeautifulSoup(req.content, 'html.parser')
            pageofpages = soup.find('div', class_='page-of-pages')
            if not pageofpages:
                proxies.remove(success_proxy)
                continue

            pageofpages = pageofpages.get_text().strip().replace('Page ', '').split(' of ')
            hasNextPage = pageofpages[0] != pageofpages[1]

            for content in soup.find_all('div', class_='review review--with-sidebar'):
                results = results.append(pd.Series(scrape_content(content), index=results.columns), ignore_index=True)   
            
            start += PER_PAGE

            print("results len:", results.shape[0], "; hasNext:", hasNextPage)
            time.sleep(0.1)

        filename = "data/reviews/{:03d}_{}.csv".format(i, yelp_rsts.loc[i]['alias'].replace("-", "_"))
        results.to_csv(filename, index=False)
        print("Export to", filename)
    


if __name__== "__main__":
    start = 0
    end = sys.maxsize
    if len(sys.argv) > 1:
        start = int(sys.argv[1])
    if len(sys.argv) > 2:
        end = int(sys.argv[2])

    run_scrapper(start, end)
