# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 09:44:01 2018

@author: Tarena
"""
from urllib import request,error
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool,Manager
def getHtml(url,ua_agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',num_retries=5):
    headers={'User-Agent':ua_agent}
    req=request.Request(url,headers=headers)
    try:
        response=request.urlopen(req)
        html=response.read().decode('utf-8')
    except error.URLError or error.HTTPError as e:
        if hasattr(e,'code') and 500<=e.code<600:
            getHtml(url,ua_agent,num_retries-1)
    return html
#print(getHtml('https://www.douban.com/doulist/3516235/?start=0&sort=seq&sub_type='))
    
def get_movie_all(html):
    """
    获取当前页面中所有的电影的列表信息
    """
    soup = BeautifulSoup(html, "html.parser")
    movie_list = soup.find_all('div', class_='bd doulist-subject')
    #print(movie_list)
    return movie_list

def get_movie_one(movie):
    """
    获取一部电影的精细信息，最终拼成一个大的字符串
    """
    result = ""
    soup = BeautifulSoup(str(movie),"html.parser")
    title = soup.find_all('div', class_="title")
    soup_title = BeautifulSoup(str(title[0]), "html.parser")
    for line in soup_title.stripped_strings:
        result += line
    
    try:
        score = soup.find_all('span', class_='rating_nums')
        score_ = BeautifulSoup(str(score[0]), "html.parser")
        for line in score_.stripped_strings:
            result += "|| 评分："
            result += line
    except:
         result += "|| 评分：5.0"
         
    abstract = soup.find_all('div', class_='abstract')
    abstract_info = BeautifulSoup(str(abstract[0]), "html.parser")
    for line in abstract_info.stripped_strings:
        result += "|| "
        result += line    
    
    result += '\n'
    #print(result)
    return result

def save_file(movieInfo):
    """
    写文件的操作,这里使用的追加的方式来写文件
    """
    with open("doubanMovie.txt","ab") as f:
        #lock.acquire()
        f.write(movieInfo.encode("utf-8"))
        #lock.release()

def CrawlMovieInfo(url):
    """
    抓取电影一页数据，并写入文件
    """
    global crawl_queue
    global crawled_queue
    html = getHtml(url)
    pattern = re.compile('(https://www.douban.com/doulist/3516235/\?start=.*)"')
    itemUrls = re.findall(pattern, html)

    for item in itemUrls:
        if item not in crawled_queue: 
            # 第一步去重，确定这些url不在已爬队列中
            crawl_queue.append(item)
    #第二步去重，对待爬队列去重
    crawl_queue = list(set(crawl_queue))
    
    movie_list = get_movie_all(html)
    for it in movie_list:
        save_file(get_movie_one(it))
    
    crawled_queue.append(url)

# 两步去重操作
crawl_queue = []    # 待爬队列
crawled_queue = []  # 已爬取队列

if __name__ == "__main__":
    pool=Pool()
    q=Manager().Queue()
    # 设置种子URL入队列
    seed_url = "https://www.douban.com/doulist/3516235/?start=0&amp;sort=seq&amp;sub_type="
    crawl_queue.append(seed_url)    
    # 模拟广度优先遍历
    while crawl_queue: #去待爬队列中取值，直到待爬队列为空
        url = crawl_queue.pop(0)#取出待爬队列中第一个值
        #用进程池中的进程来处理url
        pool.apply_async(func=CrawlMovieInfo,args=url)
        #处理完之后需要把这个url放入已爬队列
        url=q.get()
        crawled_queue.append(url)



        
    print(len(crawled_queue))     
    
    