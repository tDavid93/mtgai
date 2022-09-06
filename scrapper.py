
#pip install requests
#pip install BeautifulSoup
#pip install sqlite3

from os.path import isfile
import requests
from bs4 import BeautifulSoup
import sqlite3
import os



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


def getDeckList(url):
  baseURL= "https://tappedout.net{url}?fmt=txt"
  return requests.get(baseURL.format(url=url)).text


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
    qres = sqlCur.execute("SELECT ROWID FROM cards WHERE name= ?",(name,))
    id = qres.fetchone()
    idt = 0
    if id == type([]):
      idt = id[0]

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
      SQLcursor.execute("INSERT INTO deck_names values (?,?)", (rowId[0], url))
      sqlConnector.commit()
  SQLcursor.close()


def createDeckTables(sqlConnector):
  cur = sqlConnector.cursor()
  cur.execute("CREATE TABLE deck_names(id, name)")
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

def clearDB(sqlConnectorDeck,sqlConnectorPrints):
  cur = sqlConnectorDeck.cursor()
  cur.execute("DROP TABLE decks")
  sqlConnectorDeck.commit()
  cur.execute("DROP TABLE deck_names")
  sqlConnectorDeck.commit()
  cur.execute("DROP TABLE cards")
  sqlConnectorDeck.commit()
  cur.close()
  createDeckTables(sqlConnectorDeck)
  createCardTable(sqlConnectorDeck,sqlConnectorPrints)

def isTabelExists(sqlConnectorDeck,sqlConnectorPrints):
 tableNameList = ["decks", "deck_names", "cards"]
 sqlCursor = sqlConnectorDeck.cursor()
 for t in tableNameList:
  tableResp = sqlCursor.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name = '?'", (t,)).fetchall()


  if tableResp == []:
   clearDB(sqlConnectorDeck, sqlConnectorPrints)
   break







def main():

#kártya adatbázis letöltése ha nem létezik
 printURL = "https://mtgjson.com/api/v5/AllPrintings.sqlite"
 if os.path.isfile() != True :
  file = open("AllPrintings.sqlite")
  file.write(requests.get(printURL).text)
  file.close
 prints = sqlite3.connect("AllPrintings.sqlite")



#pakli adatbázis feécsatolása és táblák generelásáa
 con = sqlite3.connect("decks1.sqlite")
 isTabelExists(con,prints)

#pakli linkek keresése query string alapján
 queryString = "zombie"
 links = searchForLinks(q=queryString)

#paklik letöltése, feldolgozása és mentése sqlite adatbázisba 
 for l in links:

  dlist = getDeckList(url=l)
  pdlist = parseDeckList(dlist=dlist, sqlConnector=con)
  
  saveDeckToSql(url=l,deck=pdlist,sqlConnector=con)


