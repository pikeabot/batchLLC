import urllib2
import pandas as pd
from openpyxl import load_workbook
import re
from bs4 import BeautifulSoup
import threading
import logging

def soupify(url):
  user_agent = 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.63 Safari/534.3'
  headers = { 'User-Agent' : user_agent }
  req = urllib2.Request(url, None, headers)
  response = urllib2.urlopen(req)
  page = response.read()

  return BeautifulSoup(page, 'html.parser')

def start_requests():
  xl = pd.ExcelFile('test_1.xlsx')
  sheet_names = xl.sheet_names
  df = xl.parse(sheet_names[0])


  threads = []

  #for owner_name in df['OWNER_NAME_1']:
  #for i in range(0, len(df['OWNER_NAME_1'])):
  for i in range(0, 10):
    #logger.info('Business Name: {0}'.format(df['OWNER_NAME_1'][i]))
    print i
    owner_name = df['OWNER_NAME_1'][i]
    name_list = owner_name.split()
    if name_list[-1] in ['LLC', 'LTD', 'INC', 'LLLP', 'LP']:
        name_list=name_list[:-1]
    searchname=''.join(name_list)
    searchorder='%20'.join(name_list)
    url = 'http://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?inquiryType=EntityName&searchNameOrder={0}&searchTerm={1}'.format(searchname, searchorder)
    t = threading.Thread(target=get_llc_info, args=(url, i))
    threads.append(t)
    t.start()
    t.join()

def get_llc_info(url, i):
  soup = soupify(url)
  search_result = soup.find('td', class_='large-width')
  llc_soup =  soupify('http://search.sunbiz.org' + search_result.a.get('href'))

  detailed_sections = llc_soup.find_all("div", "detailSection")
  for section in detailed_sections:
    if 'Authorized' in section.get_text():
        llc_info = section.get_text()
        print llc_info
        persons = re.sub(r'Title.\S+', 'TitleAMBR', llc_info)
        persons = persons.split('TitleAMBR')
        persons.pop(0)  # Remove 'Authorized Person(s) Detail' title and other info before owners

        last_names =[]
        first_names = []
        addresses = []
        cities = []
        states = []
        zip_codes = []
        for person in persons:  
          person = re.sub('[\r\n]+|\.|[\r\n]+', '', person)
          person = re.sub('  +', ';', person)
          info = person.split(';')
          last_names.append(info[0].split(',')[0])
          first_names.append(info[0].split(',')[1])
          addresses.append(info[1])
          city_state = re.findall(r'\S+, \S+', info[2])[0].split(',')
          cities.append(city_state[0])
          states.append(city_state[1])
          zip_codes.append(re.findall(r'\d+', info[2])[0])

        print last_names
        #names.append(i.lstrip().rstrip())
        wb = load_workbook(filename = 'test_1.xlsx')
        sheet_names = wb.get_sheet_names()
        ws = wb[sheet_names[0]]
        index = str(i+2)
        ws['F'+index] = ('\n').join(last_names)
        ws['G'+index] = ('\n').join(first_names)
        ws['R'+index] = ('\n').join(addresses)
        ws['S'+index] = ('\n').join(cities)
        ws['T'+index] = ('\n').join(states)
        ws['U'+index] = ('\n').join(zip_codes)
        wb.save('test_1.xlsx')
        break  
  #self.logger.info('Index: {0}'.format(index))
  #self.logger.info('person: {0}'.format(last_names))
  return


if __name__ == '__main__':
  start_requests()