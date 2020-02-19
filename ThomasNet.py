import requests, bs4, time

base_url = "https://www.thomasnet.com/profile/"
output_file = 'ThomasNetURLS.txt'

thomas_net_ids = []

def output():
    global thomas_net_ids
    output_file = 'ThomasNetURLS.txt'
    f = open(output_file, 'a')

    for item in thomas_net_ids:
        f.write(item + '\n')
    f.close()
    print('Outputted data to ' + output_file)
    thomas_net_ids = []


#creating a persistent session for our HTTP requests
session = requests.Session()

for i in range(3100000, 4100000):
    full_url = base_url + str(i).zfill(8)
    
    try:
        #use our session to make an HTTP request
        res = session.get(full_url, timeout=5)
        #check for HTTP request errors (response<>200)
        res.raise_for_status()
    except Exception as exc:
        continue

    BS_object = bs4.BeautifulSoup(res.text, 'lxml')

    #When ThomasNet receives an invalid URL, it redirects
    #to a different landing page. This landing page does not have
    #p[class="addrline"], so we can handle this below
    if BS_object.select('p[class="addrline"]'):
        thomas_net_ids.append(full_url)
        print(full_url)
    
    #for each 1000 iteration of the loop, save data
    #(this way we don't lose data if the script crashes)
    if i % 1000 == 0:
        output()
    time.sleep(0.05)

output()

#https://www.thomasnet.com/map-company.html?cid=1008
