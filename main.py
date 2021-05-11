from progress.counter import Counter
from requests import get
from time import time
from os import mkdir
from os.path import getsize
import csv
import atexit

def ataxit_handler(allRowsForWrite):
    with open('data.csv', 'w') as csvwriter:
        writer = csv.DictWriter(csvwriter, fieldnames=['id','image_url','species_guess','scientific_name','common_name','iconic_taxon_name','taxon_id'])
        writer.writeheader()
        for row in allRowsForWrite:
            writer.writerow(row)
        
startupTime = time()
traffic = 0.0

with open('./data.csv', 'r') as csvreader:
    allRows = list(csv.DictReader(csvreader))
    allRowsForWrite = list(allRows)

    bar = Counter('Процесс загрузки: ')
    for row in allRows:
        atexit.register(ataxit_handler, allRowsForWrite=allRowsForWrite)

        pathToFolder = './result/'+row['scientific_name']
        pathToFile = pathToFolder+'/'+row['id']+'.jpeg'
        try:
            mkdir(pathToFolder)
        except FileExistsError: pass

        img = get(row['image_url'])
        imgFile = open(pathToFile, 'wb')
        imgFile.write(img.content)
        imgFile.close()

        traffic += getsize(pathToFile) / 1024 / 1024 / 1024
        
        if (traffic > 4.5):
            print('Превышение лимита трафика')
            break
        if ((startupTime + 3600 - time()) > 3600):
            startupTime = time()
            traffic = 0.0

        del allRowsForWrite[0]
        bar.next()
    bar.finish()
