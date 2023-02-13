
import os, json, gzip, traceback
import urllib.request

year = '23'

base_urls = [
	'ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/',
	'ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/'
]
dir_xml_local = './pubmed_updates_2023/'

if not os.path.exists(dir_xml_local):
	os.makedirs(dir_xml_local)

for base_url in base_urls:
	with urllib.request.urlopen(base_url) as response:
		html = response.read().decode('utf-8')
		lines = html.split('\r\n')
		for line in lines:
			line = line.strip()
			if(line.endswith('.xml.gz')):
				fpath = line.split()[-1]
				i = int(fpath.replace('.xml.gz', '').replace('pubmed{}n'.format(year),''))
				file_url = '{}{}'.format(base_url, fpath)
				opath = os.path.join(dir_xml_local, fpath)
				if not os.path.exists(opath):
					command = 'wget {} -O {}'.format(file_url, opath)
					print(command)
					os.system(command)
					while(True):
						try:
							infile = gzip.open(opath)
							content = infile.read()
							break
						except:
							command = 'wget {} -O {}'.format(file_url, opath)
							print(command)
							os.system(command)

