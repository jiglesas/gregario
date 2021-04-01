import requests

proxies = {
  'http' : '170.0.203.9:1080',
  'https': '170.0.203.9:1080',
}
url = 'https://cornershopapp.com/api/v1/branches/328/aisles/C_46/products'

#r = requests.get(url, proxies=proxies)

s = requests.Session()
s.proxies = {
  'http' : 'http://51.75.147.35:3128',
  'https': 'http://51.75.147.35:3128',
}
r = s.get(url)