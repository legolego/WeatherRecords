import inspect

# BigQuery link:
# https://console.cloud.google.com/bigquery?project=audiribtdb&folder&organizationId=1031994936391
# Storage for generated files:
# https://console.cloud.google.com/storage/browser/audiribtdb.appspot.com?project=audiribtdb

'''
Run a query
Click on Job Information above results
Click on Temporary Table link
Far right -> Click on Export -> Export to GCS
Browse/paste bucket: audiribtdb.appspot.com/WorldTemps.csv.gzip
Compression: gzip -> Export
Go to storage link above:
3 dot menu far right -> Download

'''

queryAllwban = ''
queryAllstn = ''
#Get list of wban numbers
for year in range(1943, 2020):
    query = inspect.cleandoc(f'''
            (SELECT distinct a.wban  FROM `bigquery-public-data.noaa_gsod.gsod{year}` a
            JOIN (
            SELECT *
            FROM `bigquery-public-data.noaa_gsod.stations`
            -- WHERE country='US'
            -- and state = 'MI'
            ) b
            ON a.stn=b.usaf AND a.wban=b.wban)
            '''
            )
    queryAllwban = queryAllwban + query + '\n intersect distinct \n'

print(queryAllwban)

'''
Running an intersect on the above (twice, for stn and wban) got me these station ids:
  where st.wban in 
('14739','14936','24155','25624','25308','23044','26617','25339','3940','23129','13967','23050','13743','93037','25503','13911','13739','14771','22521')
or st.usaf in
('945100','725090','727855','745940','723535','704540','703980','723810','722210','722970','723030','703260','722595','724338','725377','744910','745700','745980','722265','911820','722175','722535','722860','747686','747750','747900')


and then this:
SELECT name, usaf as stn, wban, country, state, lat, lon
  FROM `bigquery-public-data.noaa_gsod.stations` st
  where st.wban in 
('14739','14936','24155','25624','25308','23044','26617','25339','3940','23129','13967','23050','13743','93037','25503','13911','13739','14771','22521')
or st.usaf in
('945100','725090','727855','745940','723535','704540','703980','723810','722210','722970','723030','703260','722595','724338','725377','744910','745700','745980','722265','911820','722175','722535','722860','747686','747750','747900')
order by name

got me a table to duped stations with name changes, or stn/wban changes 
so I manually went through them 69 rows -> 38 rows = StationsMaster.csv
'''

#Found weather data here: https://opendata.stackexchange.com/questions/10154/sources-of-weather-data

# All temps/dates for 37 US locations available for each year 1943-2019
# Plus stn = '945100' = Charleville Airport (Charleville, Queensland) Australia
# but change gsod196* to be the year you want
query = inspect.cleandoc(f'''
SELECT name, state, country, stn, aa.wban, TIMESTAMP(CONCAT(year, '-', mo, '-', da)) date, temp as meanTemp, max as maxTemp, min as minTemp, wdsp as meanWdsp, gust
FROM `bigquery-public-data.noaa_gsod.gsod196*` aa
JOIN (
  SELECT *
  FROM `bigquery-public-data.noaa_gsod.stations` 
  -- WHERE country='US'
  -- and state='MI'
) bb 
ON aa.stn=bb.usaf AND aa.wban=bb.wban

where aa.wban in 
('14739','14936','24155','25624','25308','23044','26617','25339','3940','23129','13967','23050','13743','93037','25503','13911','13739','14771','22521')
or aa.stn in
('945100','725090','727855','745940','723535','704540','703980','723810','722210','722970','723030','703260','722595','724338','725377','744910','745700','745980','722265','911820','722175','722535','722860','747686','747750','747900')
order by country, state, name, date''')


