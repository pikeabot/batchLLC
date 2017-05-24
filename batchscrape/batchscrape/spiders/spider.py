import scrapy
import pandas as pd
import scrapy
import re
    
class BatchSpider(scrapy.Spider):
  name = "batch"
  base_url='http://search.sunbiz.org'

  def start_requests(self):
    xl = pd.ExcelFile('test_1.xlsx')
    sheet_names = xl.sheet_names
    print sheet_names
    df = xl.parse(sheet_names[0])

    name_list = df['OWNER_NAME_1'][0].split()
    if name_list[-1] in ['LLC', 'LTD', 'INC', 'LLLP', 'LP']:
        name_list=name_list[:-1]
    searchname=''.join(name_list)
    searchorder='%20'.join(name_list)
    url = 'http://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?inquiryType=EntityName&searchNameOrder={0}&searchTerm={1}'.format(searchname, searchorder)
    yield scrapy.Request(url=url, callback=self.parse_for_url)
    '''
    for url in urls:
        yield scrapy.Request(url=url, callback=self.parse)
    '''
  def parse_for_url(self, response):
    url = response.xpath('//td[@class="large-width"]//@href').extract_first()
    url = self.base_url+url
    yield scrapy.Request(url=url, callback=self.parse)

  def parse(self, response):
    print 'Getting filing info \n'
    names=[]
    #info = response.xpath('//div[@class="detailSection filingInformation"]').extract_first()
    #info = response.xpath('//*[@class="detailSection"]/span[text()[contains(.,"Authorized Person")]]').extract()
    #info = response.xpath('//*[@class="detailSection filingInformation"]/span[text()]').extract_first()
    info = response.xpath('//div[@class="detailSection"]/text()').extract()
    for i in info:
        name = re.search(r'\S', i)
        if name:
            print i.lstrip().rstrip()
            names.append(i.lstrip().rstrip())
    
    #print info

