from bs4 import BeautifulSoup as bs
from matplotlib.pyplot import title
import requests
import psycopg2

def get_book_data(url):
    ratingdict = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }
    #fetch data from the page for current book
    book_page = requests.get(url)

    #create BeautifulSoup object
    book_soup = bs(book_page.text, "lxml")
        
    #get book title
    title = book_soup.find(class_ = "col-sm-6 product_main").h1.text.strip()

    #get book price
    price = book_soup.find(class_ = "col-sm-6 product_main").p.text.strip('Â')

    #get book availability
    avail = book_soup.find(class_ = "instock availability").text.strip()

    #get book rating
    rating = ratingdict[book_soup.find(class_ = "icon-star").parent.get("class")[1]]
    
    return title, price, avail, rating

def scrape_pages(url):
    #fetch page data
    page = requests.get(url)

    #create BeautifulSoup object
    soup = bs(page.text, 'lxml')

    #get product details
    books = soup.find_all(class_ = "product_pod")

    books_in_page = []

    #loop through each book in a page
    for book in books:
        #get link to page for current book
        book_lnk = "http://books.toscrape.com/catalogue/" + book.find("h3").a.get("href")
        
        book_data = get_book_data(book_lnk)
        
        books_in_page.append(book_data)
    
    return books_in_page


# connection establishment
conn = psycopg2.connect(
    database="postgres",
    user='postgres',
    password='postgres',
    host='localhost',
    port= '5432'
)
conn.autocommit = True

cursor = conn.cursor()

#create a new database
sql = ''' CREATE database bookscrape '''

cursor.execute(sql)

conn.close()

#connect new database created
conn = psycopg2.connect(
    database="bookscrape",
    user='postgres',
    password='postgres',
    host='localhost',
    port= '5432'
)

cursor = conn.cursor()

#create books table in bookscrape database
sql = '''CREATE TABLE books (
        name VARCHAR, 
        price VARCHAR, 
        availability VARCHAR, 
        rating INTEGER
    )'''

cursor.execute(sql)

allbooks = []

#loop through all 50 pages
for i in range(1, 51):
    #page url
    url = "http://books.toscrape.com/catalogue/page-"+str(i)+".html"
    books_in_page = scrape_pages(url)
    allbooks += books_in_page

for book in allbooks:
    #insert book data into table
    cursor.execute("INSERT INTO books VALUES (%s, %s, %s, %s)", book)
    conn.commit()

#close connection        
conn.close()