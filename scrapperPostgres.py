import concurrent.futures
from os.path import isfile
import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import psycopg2

from concurrent.futures import thread


def searchForLinks(q ,deck_format="edh", price_min="", price_max="", update=""):
    #https://tappedout.net/mtg-decks/search/
# ?
# q=lagrella
# format=edh
# price_min=
# price_max=
# o=-date_updated
# submit=Filter+results
# p=2
# page=2
    baseURL = "https://tappedout.net/mtg-decks/search/?q={q}&format={deck_format}&price_min={price_min}&price_max={price_max}&o=-date_updated{update}&submit=Filter+results&p={p}&page={page}"
    req_succes = True
    page = 1
    links = []
    while req_succes:
        print(baseURL.format(p=page, page=page, q=q, deck_format=deck_format, price_min=price_min,price_max=price_max, update=update))
        webp = requests.get(baseURL.format(p=page, page=page, q=q, deck_format=deck_format, price_min=price_min,price_max=price_max, update=update))
        if webp.status_code == 200:
            page += 1
            soup = BeautifulSoup(webp.content, "html.parser")
            h3 = soup.find_all("h3", class_="name deck-wide-header")
            for h in h3:
                print(h.find("a")["href"])
                links.append(h.find("a")["href"])
        else:
            req_succes = False
    return links


def saveLinks(links, sqlConnector):
    sqlCursor = sqlConnector.cursor()

    for link in links:
        resp = sqlCursor.execute("SELECT name, fetched FROM deck_names WHERE name=?", (link,)).fetchall()
        if resp == []:
            sqlCursor.execute("INSERT INTO deck_names values (?,?,?)", (0,link,0))
            sqlConnector.commit()
    sqlCursor.close()

def fetchDecks(sqlConnector):
    sqlCursor = sqlConnector.cursor()
    fetchLinks = sqlCursor.execute("SELECT name FROM deck_names WHERE fetched=0").fetchall()
    allLinkCount = len(fetchLinks)
    progress = 0
    for link in fetchLinks:
        progress = progress + 1
        print("{prog}/{all}".format(prog=progress,all=allLinkCount))
        deck = parseDeckList(getDeckList(link[0]),sqlConnector)
        card_expanded = []
        for card in deck:
            while card[1] != 0:
                card_expanded.append(card[0])
                card[1] = card[1]-1
        print(len(card_expanded))
        if len(card_expanded) == 100:  
            sqlCursor.execute("INSERT INTO decks('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", card_expanded )
            sqlConnector.commit()
            responseRowId = sqlCursor.execute("SELECT last_insert_rowid()")
            rowId = responseRowId.fetchone()
            sqlCursor.execute("UPDATE deck_names SET fetched=1, id = ? WHERE name = ?", (rowId[0],link[0]))
            sqlConnector.commit()
        else:
            sqlCursor.execute("DELETE FROM deck_names WHERE name = ?",(link[0],))

def fetchDecksMultithread(sqlConnector):
    sqlCursor = sqlConnector.cursor()
    fetchLinks = sqlCursor.execute("SELECT name FROM deck_names WHERE fetched=0").fetchall()
    allLinkCount = len(fetchLinks)
    progress = 0
    threadCount = 10
    while len(fetchLinks) >= 0:
    
        print("{prog}/{all}".format(prog=progress,all=allLinkCount))
        if threadCount > len(fetchLinks):
            threadCount = len(fetchLinks)

        threadLinks = fetchLinks[0:threadCount]
        fetchLinks = fetchLinks[threadCount-1:]
        progress = progress + threadCount
        with concurrent.futures.ThreadPoolExecutor() as executor:
            deckLists = []
            for link in threadLinks:
                deckLists.append(executor.submit(getDeckListMultithread,link))



    
        print("decklist: {lenght}".format( lenght=len(deckLists)))
        for rDeck in concurrent.futures.as_completed(deckLists):
            dDeck = rDeck.result()
            deck = parseDeckList(dDeck[1],sqlConnector)
        
        
            card_expanded = []
            for card in deck:
                while card[1] != 0:
                    card_expanded.append(card[0])
                    card[1] = card[1]-1
            print(len(card_expanded))
            if len(card_expanded) == 100:  
                sqlCursor.execute("INSERT INTO decks('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", card_expanded )
                sqlConnector.commit()
                responseRowId = sqlCursor.execute("SELECT last_insert_rowid()")
                rowId = responseRowId.fetchone()
                sqlCursor.execute("UPDATE deck_names SET fetched=1, id = ? WHERE name = ?", (rowId[0],dDeck[0][0]))
                sqlConnector.commit()
            else:
                sqlCursor.execute("DELETE FROM deck_names WHERE name = ?",(dDeck[0][0],))



def getDeckList(url):
    baseURL= "https://tappedout.net{url}?fmt=txt"
    return requests.get(baseURL.format(url=url)).text

def getDeckListMultithread(url):
    baseURL= "https://tappedout.net{url}?fmt=txt"
    return [url,requests.get(baseURL.format(url=url[0])).text]


def parseDeckList(dlist, sqlConnector):
    sqlCur = sqlConnector.cursor()
    deck = []
    for line in dlist.splitlines():
        line = line.strip()
        if len(line) == 0:
            continue

        if not line[0] in '0123456789':
            continue

        count = line.split(' ', maxsplit=1)[0]
        name = line.split(' ', maxsplit=1)[1]
        id = sqlCur.execute("SELECT ROWID FROM cards WHERE name= ?",(name,)).fetchone()
        idt = 0
        if id != None:
            idt = id[0]
        else:
            sqlCur.execute("INSERT INTO cards(name) VALUES(?)",(name,))
            sqlConnector.commit()
            id = sqlCur.execute("SELECT ROWID FROM cards WHERE name= ?",(name,)).fetchone()
            idt=id[0]


        card = [idt ,int(count), name ]
        deck.append(card)
    sqlCur.close()
    return deck


def saveDeckToSql(url, deck, sqlConnector):
    SQLcursor = sqlConnector.cursor()
    dbCheckResponse = SQLcursor.execute("SELECT * FROM deck_names WHERE name = ?", (url,))
    dbCheck = dbCheckResponse.fetchone()
    if dbCheck != []:
        card_expanded = []
        for card in deck:
            while card[1] != 0:
                card_expanded.append(card[0])
                card[1] = card[1]-1
        print(len(card_expanded))
        if len(card_expanded) == 100:  
            SQLcursor.execute("INSERT INTO decks('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", card_expanded )
            sqlConnector.commit()
            responseRowId = SQLcursor.execute("SELECT last_insert_rowid()")
            rowId = responseRowId.fetchone()
            SQLcursor.execute("INSERT INTO deck_names values (?,?,?)", (rowId[0], url, 1))
            sqlConnector.commit()
    
    SQLcursor.close()


def createDeckTables(sqlConnector):
    cur = sqlConnector.cursor()
    cur.execute("CREATE TABLE deck_names(id, name, fetched)")
    sqlConnector.commit()
    cur.execute("CREATE TABLE decks({seq})".format(seq=', '.join(map(lambda x: '"'+x+'"',map(str,range(0,100))))))
    sqlConnector.commit()
    cur.close()

def createCardTable(sqlConnectordeck, sqlConnectorprints):
    dCur = sqlConnectordeck.cursor()
    pCur = sqlConnectorprints.cursor()
    unr = pCur.execute("SELECT DISTINCT name FROM cards ")
    uniquenames = unr.fetchall()
    dCur.execute("CREATE TABLE cards (name TEXT) ")
    dCur.executemany("INSERT INTO cards(name) VALUES(?)", uniquenames)
    sqlConnectordeck.commit()
    dCur.close()
    pCur.close()

def clearDB(sqlConnector):
    cur = sqlConnector.cursor()
    cur.execute("DROP TABLE decks")
    sqlConnector.commit()
    cur.execute("DROP TABLE deck_names")
    sqlConnector.commit()
    cur.close()
    createDeckTables(sqlConnector)