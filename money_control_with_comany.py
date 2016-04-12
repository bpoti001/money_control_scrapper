# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 16:28:42 2016

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
			data = urllib.request.urlopen(url[0]).read()
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
	coll = db.moneycontrol_withcompany
	coll.insert_one({"date":dat,"keywords":key_words,"article":text,"company":url[1]})
	client.close()
def extract_page(url):
	pa=[]
	ret=[]
	data = urllib.request.urlopen(url).read()
	#data = e.partial
	data = data.decode("utf-8",errors='ignore')
	soup = BeautifulSoup(data,'html.parser')
	d=soup.find_all('div',class_='pages MR10 MT15')
	m=soup.find_all('h1',class_='b_42 PT20')
	smt = BeautifulSoup(str(m),'html.parser')
	comp = smt.getText()
	comp = comp.replace("[","")
	comp = comp.replace("]","")
	comp = comp.strip()
	for i in d:
		g=i.find_all('a',href=True)
		if g:
			for li in g:
				pa.append(li['href'])
		else:
			pass
	for i in range(len(pa)):
		ret.append([pa[i],comp])
	return ret

def extract_article_urls(url):
	artic=[]
	data = urllib.request.urlopen(url[0]).read()
	data = data.decode("utf-8",errors='ignore') 
	soup = BeautifulSoup(data,'html.parser')
	d = soup.find_all('a',class_="arial11_summ",href=True)
	for a in d:
		artic.append([a['href'],url[1]])
	return artic

def extract_years(url):
	inter=[]
	data = urllib.request.urlopen(url).read()
	data = data.decode("utf-8",errors='ignore')
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
	print (len(names))
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
	all_p = [list(elem) for elem in all_pages]
	for i in all_p:
		i[0]=base+i[0]
	print (len(all_p))
	second_set=[]
	for i in all_p:
		if 'scat' not in i[0]:
			second_set.append(i[0])
	second = pool.map(extract_page,second_set)
	seconds = list(itertools.chain.from_iterable(second))
	for i in seconds:
		i[0]=base+i[0]
	total_page_urls = seconds+all_p
	print ("lenght of all urls for comapnes years and pages ",len(total_page_urls))
	
	article_base = 'http://www.moneycontrol.com/company-article/stocks/company_info'
	articles = pool.map(extract_article_urls,total_page_urls)
	all_articles = list(itertools.chain.from_iterable(articles))
	for i in all_articles:
		i[0]= article_base+i[0]
	pool.map(extract,all_articles)
	

	
	