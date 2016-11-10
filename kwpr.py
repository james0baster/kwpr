import sys
import psycopg2
from googleapiclient import sample_tools

import unicodecsv, datetime, csv

today = datetime.datetime.today()
tanggal = today - datetime.timedelta(weeks=1)
tanggal = tanggal.strftime("%Y-%m-%d")

uris = {'https://www.zalora.com.hk': 'hk',
        'https://zh.zalora.com.hk': 'zh',
        'https://www.zalora.co.id': 'id',
        'https://www.zalora.com.my' :'my',
        'https://www.zalora.com.ph': 'ph',
        'https://www.zalora.sg': 'sg',
        'https://www.zalora.co.th':'th',
        'https://en.zalora.co.th':'en',
        'https://www.zalora.vn':'vn',
        'https://www.zalora.com.tw' : 'tw'
        }

devices = ['desktop', 'mobile']
           
country_code = {'hk': 'hkg', 'id': 'idn', 'my': 'mys', 'ph': 'phl',
        'sg': 'sgp', 'th': 'tha', 'tw': 'twn', 'vn': 'vnm'}

def main(argv):
    service, flags = sample_tools.init(
            argv, 'webmasters', 'v3', __doc__, __file__,
            scope='https://www.googleapis.com/auth/webmasters.readonly')

    for uri in uris:
        for device in devices:

          negara = country_code[uri[-2:]]
          request1 = {
                  'startDate': tanggal,
                  'endDate': tanggal,
                  'dimensions': ['page','query'],
                  'dimensionFilterGroups': [{
                      'groupby': 'and',
                      'filters': [{
                          'dimension': 'device',
                          'expression': device
                          },
                          {
                              'dimension': 'country',
                              'expression': negara
                              }]
                          }],
                  'rowLimit': 5000
                  }

          response1 = execute_request(service, uri, request1)

          lastweek_results = {}
          for line in response1['rows']:

              key = ','.join([i.encode('utf-8') for i in line['keys']])
              lastweek_results[key] = { 'page' : line['keys'][0],
                                                    'query' : line['keys'][1],
                                                    'clicks' : line['clicks'],
                                                    'impressions': line['impressions'],
                                                    'ctr' : line['ctr'],
                                                    'position' : line['position'],
                                                    }
         

          simpan(lastweek_results, tanggal, device, negara)

    outfile.close()

def simpan(lastweek_results, tanggal, device, negara):

    for key in lastweek_results:
        obj = lastweek_results[key]
        page = obj['page']
        query = obj['query']
        query = query.replace("'", "")
        clicks = obj['clicks']
        impressions = obj['impressions']
        ctr = '{:.2f}%'.format(obj['ctr']*100)
        position = '{:.1f}'.format(obj['position'])
        conn_string = "host='rahasia' dbname='rahasia' user='rahasia' password='rahasia'"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        values = "'"+negara+"', '"+str(tanggal)+"', '"+device+"', '"+page+"', '"+query+"', "+str(impressions)+", "+str(clicks)+", '"+str(ctr)+"', "+str(position)
        statement = 'INSERT INTO seo.kwpr_gsc (country, date, device, page, query, impressions, clicks, ctr, position ) VALUES (' + values + ');'
        cursor.execute(statement)
        conn.commit()
		
	
def execute_request(service, property_uri, request):
    return service.searchanalytics().query(
            siteUrl=property_uri, body=request).execute()


if __name__ == '__main__':
    main(sys.argv)
