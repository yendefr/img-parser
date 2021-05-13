from progress.counter import Counter
from requests_futures import sessions
from concurrent.futures import as_completed, ThreadPoolExecutor
from os import mkdir
from os.path import getsize
from time import time
import csv
import atexit


def ataxit_handler(allRowsForWrite):
    with open('data.csv', 'w') as csvwriter:
        writer = csv.DictWriter(csvwriter,
                                fieldnames=['id', 'image_url', 'species_guess', 'scientific_name', 'common_name',
                                            'iconic_taxon_name', 'taxon_id'])
        writer.writeheader()
        for row in allRowsForWrite:
            writer.writerow(row)


# Я не хочу ещё час придумывать название для функции, поэтому просто продолжу кодить
# Мы же здесь деньги делаем, а не в слова играем.
def make_folder_and_file(id, scientific_name, imageContent):
    pathToFolder = './result/' + scientific_name
    pathToFile = pathToFolder + '/' + id + '.jpeg'
    try:
        mkdir(pathToFolder)
    except FileExistsError:
        pass

    imgFile = open(pathToFile, 'wb')
    imgFile.write(imageContent)
    imgFile.close()
    bar.next()


def get_images(urls):
    imagesContent = []

    with sessions.FuturesSession() as session:
        futures = [session.get(url) for url in urls]
        for future in as_completed(futures):
            response = future.result()
            imagesContent.append(response.content)

    return imagesContent


def set_images(ids, scientific_names, imagesContent, startupTime, traffic):
    with ThreadPoolExecutor(max_workers=20) as pool:
        pool.map(make_folder_and_file, ids, scientific_names, imagesContent)

    for j in range(20):
        traffic += getsize('./result/' + scientific_names[j] + '/' + ids[j] + '.jpeg') / 1024 / 1024 / 1024

    if traffic > 4.5:
        print('Превышение лимита трафика')
        exit()
    if (startupTime + 3600 - time()) > 3600:
        startupTime = time()
        traffic = 0.0

    return startupTime, traffic


startupTime = time()
traffic = 0.0

with open('./data.csv', 'r') as csvreader:
    allRows = list(csv.DictReader(csvreader))
    allRowsForWrite = list(allRows)

    i = 0
    ids = []
    urls = []
    scientific_names = []
    bar = Counter('Процесс загрузки: ')
    for row in allRows:
        atexit.register(ataxit_handler, allRowsForWrite=allRowsForWrite)

        i += 1
        if i > 20:
            imagesContent = get_images(urls)
            startupTime, traffic = set_images(ids, scientific_names, imagesContent, startupTime, traffic)
            del allRowsForWrite[:20]
            i, ids, urls, scientific_names = 1, [], [], []

        ids.append(row['id'])
        urls.append(row['image_url'])
        scientific_names.append(row['scientific_name'])

    bar.finish()
