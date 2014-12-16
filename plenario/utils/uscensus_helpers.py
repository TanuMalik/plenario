import sys
import urllib2
import zipfile
import re
import sqlalchemy 

from bs4 import BeautifulSoup

from plenario.utils.shapefile_helpers import PlenarioShapeETL

censusblocks_url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/'

state_mapping =  [
('01','Alabama'),
('02','Alaska'),
('60','American Samoa'),
('04','Arizona'),
('05','Arkansas'),
('06','California'),
('08','Colorado'),
('69','Commonwealth of the Northern Mariana Islands'),
('09','Connecticut'),
('10','Delaware'),
('11', 'District of Columbia'),
('12','Florida'),
('13','Georgia'),
('66','Guam'),
('15','Hawaii'),
('16','Idaho'),
('17','Illinois'),
('18','Indiana'),
('19','Iowa'),
('20','Kansas'),
('21','Kentucky'),
('22','Louisiana'),
('23','Maine'),
('24','Maryland'),
('25','Massachusetts'),
('26','Michigan'),
('27','Minnesota'),
('28','Mississippi'),
('29','Missouri'),
('30','Montana'),
('31','Nebraska'),
('32','Nevada'),
('33','New Hampshire'),
('34','New Jersey'),
('35','New Mexico'),
('36','New York'),
('37','North Carolina'),
('38','North Dakota'),
('39','Ohio'),
('40','Oklahoma'),
('41','Oregon'),
('42','Pennsylvania'),
('72','Puerto Rico'),
('44','Rhode Island'),
('45','South Carolina'),
('46','South Dakota'),
('47','Tennessee'),
('48','Texas'),
('78','United States Virgin Islands'),
('49','Utah'),
('50','Vermont'),
('51','Virginia'),
('53','Washington'),
('54','West Virginia'),
('55','Wisconsin'),
('56','Wyoming')]

def _uscensusblocks_get_all_census_hrefs():
        html = urllib2.urlopen(censusblocks_url).read()
        soup=BeautifulSoup(html)
        hrefs = [a.attrs.get('href') for a in soup.select('td a')]
        return hrefs

def _uscensusblocks_get_all_state_codes():
        # basically go through this list and for all files like 'tl_2010_01_tabblock.zip' , find 
        # all files like tl_2010_01[0-9][0-9][0-9]_tabblock.zip and append them to a list
        hrefs = _uscensusblocks_get_all_census_hrefs()
        prefix ='tl_2010_([0-9][0-9])_.*'
        state_codes = []
        for href in hrefs:
                m= re.search(prefix, href)
                if (m):
                        state_code = m.group(1)
                        state_codes.append(state_code)
        return sorted(state_codes)


def _uscensusblocks_get_all_county_zipfiles(state_num_str):
        hrefs = _uscensusblocks_get_all_census_hrefs()
	ret_list = []
	for href in hrefs:
		m = re.search('tl_2010_%s([0-9][0-9][0-9])_.*' % state_num_str, href)
		if (m):
			ret_list.append(href)	
	return ret_list

def uscensusblocks_add_state_byname(state_name):
    state_name.lower()
    state_mapping_dict = dict(state_mapping)
    inv_state_mapping_dict = dict((v.lower(),k) for k, v in state_mapping_dict.iteritems())
    try:
        uscensusblocks_add_state(inv_state_mapping_dict[state_name])
    except KeyError, e:
        print "State not found: '%s'" % state_name

def uscensusblocks_add_state(state_id):
    zip_list = _uscensusblocks_get_all_county_zipfiles(state_id)
    
    for fname in zip_list:
        url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/%s' % fname
        d = {'dataset_name': 'census_blocks', 'business_key': 'geoid10', 'source_url': url}
        #print "doing d", d
        e = PlenarioShapeETL(d)
        #print "downloading"
        e._download()
        try:
            #print "loading shapefile"
            e._load_shapefile()
        except zipfile.BadZipfile, e:
            #print "no data found for %s, skipping" % url
            continue
        #print "getting/creating table"
        e._get_or_create_table()
        #print "inserting data"
        try:
            e._insert_data()
        except sqlalchemy.exc.IntegrityError, e:
            print "got integrity error, skipping", e
        #print "done inserting data"
    
def uscensusblocks_add_all():
    for (sid, sname) in state_mapping:
        #print "adding censusblocks for ", (sid, sname)
        uscensus_add_state(sid)

