# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 20:48:29 2021

@author: Yash Sharma
"""

from Plot_AQI import avg_data_2013, avg_data_2014, avg_data_2015,avg_data_2016
import requests
import sys
import pandas as pd
from bs4 import BeautifulSoup
import os
import csv

def met_data(month,year):
    
    file_html = open('Data/Html_Data/{}/{}.html'.format(year,month), 'rb')
    plain_txt = file_html.read()
    
    temp_data=[]
    final_data=[]
    
    soup = BeautifulSoup(plain_txt, 'lxml')
    for table in soup.findAll('table', {'class': 'medias mensuales numspan'}):
        for tbody in table:
            for tr in tbody:
                a = tr.get_text()
                temp_data.append(a)
    rows = len(temp_data)/15 # there are 15 features in the html page to find no. of rows
    
    for times in range(round(rows)):
        new_temp_data=[]
        for i in range(15):
            new_temp_data.append(temp_data[0])
            temp_data.pop(0)
        final_data.append(new_temp_data)
        
    length = len(final_data)
    
    final_data.pop(length-1)
    final_data.pop(0)
    
    for a in range(len(final_data)):
        final_data[a].pop(6)
        final_data[a].pop(13)
        final_data[a].pop(12)
        final_data[a].pop(11)
        final_data[a].pop(10)
        final_data[a].pop(9)
        final_data[a].pop(0)
    return final_data


def data_combine(year, cs):
    for a in pd.read_csv('Data/Real-Data/real_' + str(year) + '.csv', chunksize=cs):
        df = pd.DataFrame(data = a)
        mylist = df.values.tolist()
    return mylist 

if __name__ == '__main__':
    if not os.path.exists('Data/Real-Data'):
        os.makedirs('Data/Real-Data')
    for year in range(2013, 2017):
        final_data=[]
        with open('Data/Real-Data/real_' + str(year) + '.csv', 'w') as csvfile: # if csv file not created , create it
            wr = csv.writer(csvfile, dialect='excel')
            wr.writerow(['T', 'TM', 'Tm', 'SLP', 'H', 'VV', 'V', 'VM', 'PM 2.5'])
        for month in range(1,13):  # this loop extract the data of 12 months for each year
            temp = met_data(month, year) 
            final_data+=temp
        pm = getattr(sys.modules[__name__],'avg_data_{}'.format(year))() # dynamic way to call all avg_data which are imported above
                                                                        # to get my feature PM2.5                        
        #print(type(pm))
        if len(pm) == 364:
            pm.insert(364,'-')
        for i in range(len(final_data)-1):
            final_data[i].insert(8, pm[i]) # it insert pm2.5 at last column
        
        with open('Data/Real-Data/real_' + str(year) + '.csv', 'a') as csvfile: # this loop will insert the final_data in real_year folder 
            wr = csv.writer(csvfile,dialect='excel')
            for row in final_data:
                flag = 0
                for elem in row:
                    if elem == "" or elem == '-':
                        flag = 1
                    if flag!= 1:
                        wr.writerow(row)
    
        
    data_2013 = data_combine(2013, 600)
    data_2014 = data_combine(2014, 600)  
    data_2015 = data_combine(2015, 600)
    data_2016 = data_combine(2016, 600)
    
    total = data_2013 + data_2014 + data_2015 + data_2016
    
    with open('Data/Real-Data/Real_Combine.csv', 'w') as csvfile: # combine all the year data in new file
        wr = csv.writer(csvfile, dialect='excel')
        wr.writerow(['T', 'TM', 'Tm', 'SLP', 'H', 'VV', 'V', 'VM', 'PM 2.5'])
        wr.writerows(total)
    
    
   