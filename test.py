from bs4 import BeautifulSoup
import requests
import pg8000 as dbapi

path = "D:/viki"
url_page_pointer = "https://ru.wikipedia.org/wiki/%D0%92%D0%B8%D0%BA%D0%B8%D0%BF%D0%B5%D0%B4%D0%B8%D1%8F:%D0%90%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D0%BD%D1%8B%D0%B9_%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D1%82%D0%B5%D0%BB%D1%8C"
domen = "https://ru.wikipedia.org"
errors_values = []

def connection_db():
    conn = None
    try:
        conn = dbapi.connect(database="viki",host="localhost", port=5432, user="postgres", password="1",ssl=False)
        print("connect\n")
    except Exception as err:
        print(err)
    return conn


def reload_bd():
    conn = connection_db()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE letters;")
    conn.commit()
    cursor.execute("CREATE TABLE letters (id serial primary key, letter char(5));")
    conn.commit()
    cursor.execute("DROP TABLE pages;")
    conn.commit()
    cursor.execute("CREATE TABLE pages (id serial primary key, letter int REFERENCES letters(id), title char(100), page text);")
    conn.commit()
    conn.close()


def writer_table_letters_in_bd(values, cursor, conn, id):
    cursor.execute("INSERT INTO public.letters (id, letter) VALUES (" + str(id) + ", '" + str(values) + "');")
    conn.commit()


def get_links_pages_on_one_letter():
    page = requests.get(url_page_pointer)
    soup_page = BeautifulSoup(page.text, "html.parser")
    soup_page = soup_page.find_all("a", class_="external text")
    return soup_page


def get_links_pages_with_information(link):
    page = requests.get(link)
    soup_page = BeautifulSoup(page.text, "html.parser")
    soup_page = soup_page.find("div", class_="mw-allpages-body")
    soup_page = soup_page.find_all("li")
    return soup_page


def write_information_page_in_bd(link, id, title, cursor, conn):
    link = domen + link
    page = requests.get(link)
    soup_page = BeautifulSoup(page.text, "html.parser")
    soup = soup_page.find("div", class_="mw-body")
    soup = str(soup)
    soup = soup.replace("'", "''")
    query = "INSERT INTO public.pages (letter, title, page) VALUES (" + str(id) + ",'" + str(title) +"'"+ ",'"+ str(soup) +"'::text);"
    cursor.execute(query)
    conn.commit()


"""main"""
i = 1
f = 1
reload_bd()
conn = connection_db()
cursor = conn.cursor()
soup = get_links_pages_on_one_letter()
while True:
    information = soup[i]
    try:
        writer_table_letters_in_bd(information.get_text(), cursor, conn, i)
    except:
        continue
    soup_page = get_links_pages_with_information(information["href"])
    for j in soup_page:
        soup_end_page = j.find("a")
        try:
            write_information_page_in_bd(soup_end_page["href"], i, soup_end_page["title"], cursor, conn)
            print(str(f) + " " + soup_end_page["title"] + " \n")
            f += 1
        except:
            errors_values.append(soup_end_page["href"])
            for g in errors_values:
                print("-------------------------" + g)

    i += 1
conn.close()
print(errors_values)













