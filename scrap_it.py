import datetime
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool, Manager

root_url = "https://shop.rewe.de"
headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0"}

manager = Manager()
all_products_links = manager.list()


def get_categories():
    categories = []
    response = requests.get(root_url, headers=headers)
    soup = BeautifulSoup(response.text, features="html.parser")
    main_holder = soup.find("ul", {"id": "rs-primary-navigation"})
    all_products = main_holder.find_all("li")[1]
    all_products_ul = all_products.find_all("ul")[0]
    for li in all_products_ul.contents:
        link = li.find("a").get("href")
        if link:
            categories.append(link)
    return categories


def get_page_product_links(url="", parsed_page_res=None):
    if parsed_page_res is None:
        page_res = requests.get(url, headers=headers)
        parsed_page_res = BeautifulSoup(page_res.text, features="html.parser")
    # Get all products from first page
    all_prod_items_on_page = parsed_page_res.find_all("div", {"class": "search-service-productDetailsWrapper"})
    # walk over page items and get links to the product page
    for prod_div in all_prod_items_on_page:
        link = prod_div.next.get("href")
        all_products_links.append(link)


def get_all_product_links(category_link):
    main_category_url = "{}{}?objectsPerPage=80".format(root_url, category_link)
    page_res = requests.get(main_category_url, headers=headers)
    parsed_page_res = BeautifulSoup(page_res.text, features="html.parser")
    get_page_product_links(parsed_page_res=parsed_page_res)

    # Check if there are more pages
    pages_tags = parsed_page_res.find_all("a", {"class": "search-service-paginationPage"})
    if pages_tags:
        max_pages = int(pages_tags[-1].text)
        pools_number = max_pages-9
        if pools_number > 4:
            pools_number = 4
        pool = Pool(pools_number)
        pages = []
        for page_num in range(2, max_pages+1):
            pages.append("{}&page={}".format(main_category_url, page_num))
        pool.map(get_page_product_links, pages)
        pool.terminate()
        pool.join()


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print("Start time: ", start_time)
    categories = get_categories()
    for category in categories:
        get_all_product_links(category)
    end_time = datetime.datetime.now()
    print("Finish time: ", end_time)
    print("Products in total:", len(all_products_links))


# Start time:  2019-09-20 09:17:25.629586
# Finish time:  2019-09-20 09:32:15.270637
# Products in total: 160647
