# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 00:29:42 2016

@author: bhavyateja
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 13:53:17 2016

@author: bhavyateja
""" 
from urllib.parse import quote
import requests
import itertools
import json
import urllib.request
from parsel import Selector
import itertools
from multiprocessing import Pool
from bs4 import BeautifulSoup
import http.client
from pymongo import MongoClient
from lxml import etree

def extract(url):
	for _ in range(5):
		try:
			data = urllib.request.urlopen(url).read()
			break
		except http.client.IncompleteRead:
			pass
	data = data.decode("utf-8",errors='ignore')
	data = data.replace("</p>","")
	data = data.replace("<p>","")
	soup = BeautifulSoup(data,'html.parser')
	d=soup.find_all('div',class_='arti_cont')
	ds = BeautifulSoup(str(d),'html.parser')
	mt = ds.find_all('div',class_='MT20')
	dmt = BeautifulSoup(str(mt),'html.parser')
	for i in dmt.find_all('style'):
		i.extract()
	for i in dmt.find_all('script'):
		i.extract()
	text = dmt.getText()
	text = text.replace("[","")
	text = text.replace("]","")
	text = text.strip()
	tree = etree.HTML(data)
	key_words = tree.xpath( "//meta[@name='news_keywords']" )[0].get("content")
	date = tree.xpath( "//meta[@http-equiv='Last-Modified']" )[0].get("content")
	date = date.split()
	dat = date[0]+date[1]
	client = MongoClient()
	db = client.articles
	coll = db.moneycontrol
	coll.insert_one({"date":dat,"keywords":key_words,"article":text})
	client.close()
def extract_page(url):
    pa=[]
    data = urllib.request.urlopen(url).read()
    #data = e.partial
    data = data.decode("utf-8",errors='ignore')
    soup = BeautifulSoup(data,'html.parser')
    d=soup.find_all('div',class_='pages MR10 MT15')
    for i in d:
        g=i.find_all('a',href=True)
        if g:
            for li in g:
                pa.append(li['href'])
        else:
            pass
    return pa

def extract_article_urls(url):
    artic=[]
    data = urllib.request.urlopen(url).read()
    data = data.decode("utf-8",errors='ignore') 
    soup = BeautifulSoup(data,'html.parser')
    d = soup.find_all('a',class_="arial11_summ",href=True)
    for a in d:
        artic.append(a['href'])
    return artic

def extract_years(url):
	inter=[]
	data = urllib.request.urlopen(url).read()
	data = data.decode("utf-8") 
	soup = BeautifulSoup(data,'html.parser')
	d=soup.find_all('div',class_='FR yrs')
	for i in d:
		g=i.find_all('a',href=True)
		for li in g:
			inter.append(li['href'])
	return inter[1:]
if __name__ == '__main__':
    fi = open('companies.txt','r')
    names =[]
    for line in fi:
    	names.append(line.strip())
    url_name = []
    for i in names:
    	url_name.append(quote(i))
    query = 'http://www.moneycontrol.com/mccode/common/autosuggesion.php?query='
    rest = '&type=3&format=json'
    req = []
    for i in url_name:
    	req.append(query+i+rest)
    url=[]
    for i in req:
        data = requests.get(i)
        if data.text:
            a = data.text
            b = a.lstrip("(").rstrip(")")
            jpar = json.loads(b)
            for i in jpar:
              url.append(i['link_src']) 
    print("total number of urls came back from search",len(url))
    baseurl_company_articles = 'http://www.moneycontrol.com/company-article'
    links=[]
    base = 'http://www.moneycontrol.com'
    pool = Pool(16)
    years=[]
    years = pool.map(extract_years,url)
    print ("len of urls with years in list of lists",len(years))
    all_years = list(itertools.chain.from_iterable(years))
    print ("len of urls with years in single list",len(all_years))
    for i in range(len(all_years)):
        all_years[i]=base+all_years[i]
    pages=[]
    #pool = Pool(4)
    pages= pool.map(extract_page,all_years)
    print ("list of list of pages",len(pages))
    all_pages=list(itertools.chain.from_iterable(pages))
    for i in range(len(all_pages)):
        all_pages[i]=base+all_pages[i]
    print (len(all_pages))
    second_set=[]
    for i in all_pages:
        if 'scat' not in i:
            second_set.append(i)
    second = pool.map(extract_page,second_set)
    seconds = list(itertools.chain.from_iterable(second))
    for i in seconds:
        if 'scat' not in i:
            seconds.remove(i)
    for i in  range(len(seconds)):
        seconds[i]=base+seconds[i]
    print(len(seconds))
    total_page_urls = seconds+all_pages
    print ("lenght of all urls for comapnes years and pages ",len(total_page_urls))
    article_base = 'http://www.moneycontrol.com/company-article/stocks/company_info'
    articles = pool.map(extract_article_urls,total_page_urls)
    print (len(articles))
    all_articles = list(itertools.chain.from_iterable(articles))
    for i in range(len(all_articles)):
        all_articles[i] = article_base+all_articles[i]
    #pool = Pool(4)
    pool.map(extract,articles)