import requests, time, bs4
from bs4 import BeautifulSoup
import pandas as pd
import re

#Which URL to start at 
start       =   306136

#How many to scrape
increment   =   1000

input_file  =   r'Q:\CodeBase\Python\Webscraping\BS4\ThomasNet\ThomasNetURLS.txt'
output_file =   r'Q:\CodeBase\Python\Webscraping\BS4\ThomasNet\ThomasNet_URLs_output.csv'

#Read from list of ThomasNet URLs to scrape
urls = pd.read_csv(input_file)
#only look at a subset of the full list
#urls = urls.iloc[start:start+increment]
urls = urls.iloc[start:]

#this is a dict that will store all company's data
#and be created into a Pandas DataFrame
all_company_data = {}
count = 0

#Return text from a soup object, if the object exists
def return_text(soup_obj):
    #this handles returns from .find_all which is a list like object
    if isinstance(soup_obj, bs4.element.ResultSet):
        return ';'.join([item.text for item in soup_obj])
    else:
        return '' if (soup_obj is None or soup_obj == '') else str(soup_obj.text)

#Output existing data from all_company_data to csv file 
def output_data():
    global all_company_data
    print('Outputting Data to {}'.format(output_file))
    output_df = pd.DataFrame.from_dict(all_company_data, orient='index')
    output_df.to_csv(output_file, mode='a', header=False)
    
    #since we outputted the data, make all_company_data empty
    all_company_data = {}

count = 0

#creating a persistent session for our HTTP requests
session = requests.Session()
#Loop through the DataFrame of all URLs
for row in urls.itertuples():
       
    #If the URL is not valid, skip to the next iteration of the for loop
    try:
        res = session.get(row[1], timeout=5)
        res.raise_for_status()
    except:
        count += 1
        continue
    soup = BeautifulSoup(res.text, 'lxml')

    print(row[1])

    #Beautiful soup element scraping
    if soup.find('div', {'class':'codetail'}):
        company = return_text(soup.find('div', {'class':'codetail'}).find('h1', recursive=False))
    else:
        count += 1
        continue
    company_desc = soup.find('div', {'id':'copro_about'})

    if company_desc and company_desc.find('p', recursive=False):
        company_desc = return_text(company_desc.find('p', recursive=False))

    phone = return_text(soup.find('p', {'class':'phoneline'}))
    phone = phone.replace('Call: ', '')

    ann_sales = soup.find('div', {'class':'label'}, text = 'Annual Sales:').next_sibling.next_sibling
    if ann_sales:
        ann_sales = ann_sales.text
    else:
        ann_sales = ''
        
    num_employees = soup.find('div', {'class':'label'}, text = 'No of Employees:').next_sibling.next_sibling.text

    year_founded = soup.find('div', {'class':'label'}, text = 'Year Founded:').next_sibling.next_sibling.text

    company_type = soup.find('div', {'class':'label'}, text = 'Primary Company Type:').next_sibling.next_sibling.text

    url = soup.find('div', {'class':'label'}, text = 'Website:').next_sibling.next_sibling.find('a')
    if url:
        url = url['href']
    else:
        url = ''
        
    address_list = return_text(soup.find('p', {'class':'addrline'}))
    address_list = address_list.replace(' | map', '').split(',')

    if len(address_list) > 1:
        city = address_list[0]
        state = address_list[1].split()[0]
        zip = address_list[1].split()[1]
    else:
        city = ''
        state = ''
        zip = ''
        
    products_list = soup.find('div', {'class':'prodserv_group'})
    if products_list:
        products_list = return_text(products_list.find_all('li'))
    
    #store this company's soup data into a dict.
    company_data =  {
                        'company' : company,
                        'company_desc': company_desc,
                        'phone' : phone,
                        'ann_sales' : ann_sales,
                        'num_employees' : num_employees,
                        'year_founded' : year_founded,
                        'company_type' : company_type,
                        'url' : url,
                        'city' : city,
                        'state' : state,
                        'zip' : zip,
                        'products_list' : products_list
                    }

    all_company_data[row[1]] = company_data
    
    count += 1

    if count % 500 == 0:
        output_data()

    #Pause so we don't hit the server too frequently
    time.sleep(0.05)

    
output_data()
