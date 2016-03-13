# encoding: utf8

import re
import json
import os
import os.path
from time import sleep
from hashlib import sha1
from collections import namedtuple, defaultdict, Counter
from datetime import datetime

import requests
requests.packages.urllib3.disable_warnings()

import pandas as pd

import seaborn as sns
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib import rc
import matplotlib.ticker as mtick
# For cyrillic labels
rc('font', family='Verdana', weight='normal')

from bs4 import BeautifulSoup

from IPython.display import display, HTML


ROOT_TABLE_2001 = 'http://www.kirov.vybory.izbirkom.ru/region/region/kirov?action=show&root=1&tvd=100100028713304&vrn=100100028713299&region=43&global=&sub_region=43&prver=0&pronetvd=null&vibid=100100028713304&type=233'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'
}
DATA_DIR = 'data'
HTML_DIR = os.path.join(DATA_DIR, 'html')
HTML_LIST = os.path.join(HTML_DIR, 'list.txt')
ADDRESSES_DIR = os.path.join(DATA_DIR, 'address')
ADDRESSES_LIST = os.path.join(ADDRESSES_DIR, 'list.txt')

ROWS_2011 = [
    u'Число избирателей, внесенных в список избирателей',
    u'Число избирательных бюллетеней, полученных участковой избирательной комиссией',
    u'Число избирательных бюллетеней, выданных избирателям, проголосовавшим досрочно',
    u'Число избирательных бюллетеней, выданных избирателям в помещении для голосования',
    u'Число избирательных бюллетеней, выданных избирателям вне помещения для голосования',
    u'Число погашенных избирательных бюллетене',
    u'Число избирательных бюллетеней в переносных ящиках для голосования',
    u'Число избирательных бюллетеней в стационарных ящиках для голосования',
    u'Число недействительных избирательных бюллетеней',
    u'Число действительных избирательных бюллетеней',
    u'Число открепительных удостоверений, полученных участковой избирательной комиссие',
    u'Число открепительных удостоверений, выданных избирателям на избирательном участк',
    u'Число избирателей, проголосовавших по открепительным удостоверениям на избирательном участке',
    u'Число погашенных неиспользованных открепительных удостоверений',
    u'Число открепительных удостоверений, выданных избирателям территориальной избирательной комиссией',
    u'Число утраченных открепительных удостоверений',
    u'Число утраченных избирательных бюллетене',
    u'Число избирательных бюллетеней, не учтенных при получени',
    None,
    u'Политическая партия СПРАВЕДЛИВАЯ РОССИЯ',
    u'Политическая партия "Либерально-демократическая партия России"',
    u'Политическая партия "ПАТРИОТЫ РОССИИ"',
    u'Политическая партия "Коммунистическая партия Российской Федерации"',
    u'Политическая партия "Российская объединенная демократическая партия "ЯБЛОКО"',
    u'Всероссийская политическая партия "ЕДИНАЯ РОССИЯ"',
    u'Всероссийская политическая партия "ПРАВО"',
]
UIK_CELLS_2011 = os.path.join(DATA_DIR, 'uik_cells_2011.json')
BLUE = '#4a71b2'
RED = '#c64d4d'
GREEN = '#52c666'
PURPLE = '#8170b4'
UIKS = os.path.join(DATA_DIR, 'cik_uik_20160229.csv')
MAP_DATA = os.path.join(DATA_DIR, 'map_data.csv')
DATA = os.path.join(DATA_DIR, 'data.csv')


UikCell = namedtuple('UikCell', ['id', 'feature', 'number', 'value'])
RawIkRecord = namedtuple(
    'RawIkRecord',
    ['id', 'iz_id', 'parent_id',
     'type', 'name', 'region', 'address']
)
IkRecord = namedtuple(
    'IkRecord',
    ['id', 'parent', 'region', 'name', 'address']
)
VotesFor = namedtuple(
    'VotesFor',
    ['sr', 'ldpr', 'pr', 'kprf', 'apple', 'er', 'pravo']
)
UikStats = namedtuple('UikStats', ['size', 'total', 'votes', 'votes_for'])
Uik = namedtuple(
    'Uik',
    ['region', 'number', 'parent', 'address', 'stats']
)
Coordinates = namedtuple('Coordinates', ['longitude', 'latitude'])
Address = namedtuple('Address', ['description', 'coordinates'])
Link = namedtuple('Link', ['url', 'text'])


def log_progress(sequence, every=None, size=None):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = size / 200     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{index} / ?'.format(index=index)
                else:
                    progress.value = index
                    label.value = u'{index} / {size}'.format(
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = str(index or '?')


def jobs_manager():
    from IPython.lib.backgroundjobs import BackgroundJobManager
    from IPython.core.magic import register_line_magic
    from IPython import get_ipython
    
    jobs = BackgroundJobManager()

    @register_line_magic
    def job(line):
        ip = get_ipython()
        jobs.new(line, ip.user_global_ns)

    return jobs


def kill_thread(thread):
    import ctypes
    
    id = thread.ident
    code = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(id),
        ctypes.py_object(SystemError)
    )
    if code == 0:
        raise ValueError('invalid thread id')
    elif code != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(id),
            ctypes.c_long(0)
        )
        raise SystemError('PyThreadState_SetAsyncExc failed')


def get_chunks(sequence, count):
    count = min(count, len(sequence))
    chunks = [[] for _ in range(count)]
    for index, item in enumerate(sequence):
        chunks[index % count].append(item) 
    return chunks


def hash_item(item):
    return sha1(item.encode('utf8')).hexdigest()


hash_url = hash_item


def load_items_cache(path):
    with open(path) as file:
        for line in file:
            line = line.decode('utf8').strip()
            # In case cache is broken with unstripped item
            if '\t' in line:
                hash, item = line.split('\t', 1)
                yield item


def update_items_cache(item, path):
    with open(path, 'a') as file:
        hash = hash_item(item)
        file.write('{hash}\t{item}\n'.format(
            hash=hash,
            item=item.encode('utf8')
        ))


def get_html_filename(url):
    return '{hash}.html'.format(
        hash=hash_url(url)
    )


def get_html_path(url):
    return os.path.join(
        HTML_DIR,
        get_html_filename(url)
    )


def list_html_cache():
    return load_items_cache(HTML_LIST)


def update_html_cache(url):
    update_items_cache(url, HTML_LIST)


def dump_html(url, html):
    path = get_html_path(url)
    if html is None:
        html = ''
    with open(path, 'w') as file:
        file.write(html)
    update_html_cache(url)


def load_html(url):
    path = get_html_path(url)
    with open(path) as file:
        return file.read()


def download_url(url):
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=5
        )
        return response.content
    except requests.RequestException:
        return None


def fetch_url(url):
    html = download_url(url)
    dump_html(url, html)
    return html


def fetch_urls(urls):
    for url in urls:
        fetch_url(url)


def get_soup(html):
    return BeautifulSoup(html, 'lxml')


def get_child_region_links(html):
    if 'TEXT-DECORATION: none' in html:
        soup = get_soup(html)
        for item in soup.find_all('a', style='TEXT-DECORATION: none'):
            yield Link(item['href'], item.text)


def parse_uik_table(url, html):
    id = re.search(r'vibid=(\d+)', url).group(1)
    soup = get_soup(html)
    rows = soup.find('td', width='90%').find_all('tr')
    header = []
    for cell in rows[0].find_all('td'):
        uik = cell.find('nobr').text
        header.append(uik)
    for index, row in enumerate(rows[1:]):
        feature = ROWS_2011[index]
        if feature is not None:
            cells = row.find_all('td')
            for index, cell in enumerate(cells):
                uik = header[index]
                value = int(cell.find('b').text)
                yield UikCell(id, feature, uik, value)


def load_uik_tables(urls):
    for url in urls:
        html = load_html(url)
        for cell in parse_uik_table(url, html):
            yield cell


def dump_json_data(data, path):
    with open(path, 'w') as file:
        json.dump(data, file)


def load_json_data(path):
    with open(path) as file:
        return json.load(file)


def dump_uik_cells(cells):
    dump_json_data(cells, UIK_CELLS_2011)


def load_uik_cells(path):
    for id, index, number, value in load_json_data(path):
        yield UikCell(id, ROWS_2011[index], number, value)


def read_csv(path):
    table = pd.read_csv(path)
    return table.where(pd.notnull(table), None)


def load_raw_uiks():
    table = read_csv(UIKS)
    for index, row in table.iterrows():
        id = row.id
        if id is not None:
            id = int(id)
        iz_id = row.iz_id
        if iz_id is not None:
            iz_id = int(iz_id)
        reserve_iz_id = row.reserve_iz_id
        if reserve_iz_id is not None:
            reserve_iz_id = int(reserve_iz_id)
        parent_id = row.parent_id
        if parent_id is not None:
            parent_id = int(parent_id)
        name = row['name']
        if name:
            name = name.decode('utf8')
        region = row.region
        address = row.address
        if address:
            address = address.decode('utf8')
        type = row.type_ik
        yield RawIkRecord(
            id, iz_id, parent_id,
            type, name, region, address
        )


def load_uiks(coordinates):
    iks = {}
    tiks = {}
    uiks = {}
    for record in load_raw_uiks():
        region = record.region
        id = record.id
        iz_id = record.iz_id
        parent_id = record.parent_id
        address = record.address
        if address is not None:
            address = Address(address, coordinates.get(address))
        ik = IkRecord(iz_id, parent_id, region, record.name, address)
        type = record.type
        if type == 'ik':
            iks[id] = ik
        elif type == 'tik':
            tiks[id] = ik
        elif type == 'uik':
            uiks[id] = ik
    for id, record in tiks.iteritems():
        tiks[id] = record._replace(parent=iks[record.parent])
    for id, record in uiks.iteritems():
        yield record._replace(parent=tiks[record.parent])


def call_geocoder(address):
    response = requests.get(
        'http://geocode-maps.yandex.ru/1.x/',
        params={
            'format': 'json',
            'geocode': address
        }
    )
    if response.status_code == 200:
        return response.json()


def get_address_filename(address):
    return '{hash}.json'.format(
        hash=hash_address(address)
    )


def get_address_path(address):
    return os.path.join(
        ADDRESSES_DIR,
        get_address_filename(address)
    )


def list_addresses_cache():
    return load_items_cache(ADDRESSES_LIST)


def update_addresses_cache(address):
    return update_items_cache(address, ADDRESSES_LIST)


def dump_address(address, data):
    path = get_address_path(address)
    with open(path, 'w') as file:
        json.dump(data, file)
    update_addresses_cache(address)


def load_address(address):
    path = get_address_path(address)
    with open(path) as file:
        return json.load(file)


def parse_address(data):
    if data and 'response' in data:
        items = data['response']['GeoObjectCollection']['featureMember']
        if items:
            point = items[0]['GeoObject']['Point']['pos']
            longitude, latitude = point.split(' ', 1)
            longitude = float(longitude)
            latitude = float(latitude)
            return Coordinates(longitude, latitude)


def load_coordinates(address):
    data = load_address(address)
    return parse_address(data)


def geocode_address(address):
    data = call_geocoder(address)
    dump_address(address, data)


def geocode_addresses(addresses):
    for address in addresses:
        geocode_address(address)


def get_uik_number_by_name(name):
    if name:
        # udmurt style
        match = re.search(ur'№(\d+)\/(\d+)', name, re.U)
        if match:
            return int(match.group(1) + match.group(2))
        match = re.search(r'\d+', name)
        if match:
            return int(match.group())


def join_uiks_cells(uiks, cells):
    regions = {}
    for uik in uiks:
        id = str(uik.parent.parent.id)[:7]
        region = uik.region
        regions[id] = region
    # samara and ingush for some reason have bad ids
    regions['2632000'] = regions.pop('1001000') # pop 'samara'
    regions['2062000'] = regions.pop('2062001') # pop 'ingush'
    stats = defaultdict(dict)
    for cell in cells:
        id = str(cell.id)[:7]
        region = regions[id]
        stats[region, cell.number][cell.feature] = cell.value
    mapping = {}
    for uik in uiks:
        number = get_uik_number_by_name(uik.name)
        if number:
            mapping[uik.region, number] = uik
    for (region, number), uik in mapping.iteritems():
        if (region, number) in stats:
            data = stats[region, number]
            votes_for = VotesFor(
                data[u'Политическая партия СПРАВЕДЛИВАЯ РОССИЯ'],
                data[u'Политическая партия "Либерально-демократическая партия России"'],
                data[u'Политическая партия "ПАТРИОТЫ РОССИИ"'],
                data[u'Политическая партия "Коммунистическая партия Российской Федерации"'],
                data[u'Политическая партия "Российская объединенная демократическая партия "ЯБЛОКО"'],
                data[u'Всероссийская политическая партия "ЕДИНАЯ РОССИЯ"'],
                data[u'Всероссийская политическая партия "ПРАВО"']
            )
            uik_stats = UikStats(
                data[u'Число избирателей, внесенных в список избирателей'],
                data[u'Число избирательных бюллетеней, полученных участковой избирательной комиссией'],
                data[u'Число действительных избирательных бюллетеней'],
                votes_for
            )
            yield Uik(
                region,
                number,
                uik.parent,
                uik.address,
                uik_stats
            )


def show_votes_share_distribution(uiks):
    data = Counter()
    for uik in uiks:
        stats = uik.stats
        share = float(stats.votes) / stats.total
        share = round(share, 3)
        data[share] += 1
    table = pd.Series(data)
    fig, ax = plt.subplots()
    ax.plot(table.index, table.values, linewidth=0.5)
    ax.set_xlim(0.2, 1.05)
    ax.set_ylim(0, 400)
    formater = mtick.FuncFormatter(
        lambda value, _: '{}%'.format(int(value * 100))
    )
    ax.xaxis.set_major_formatter(formater)
    ax.set_xlabel(u'Явка')
    ax.set_ylabel(u'Число участков')
    fig.savefig('fig.png', dpi=300, bbox_inches='tight')


def show_party_shares(uiks):
    data = []
    for uik in uiks:
        votes_for = uik.stats.votes_for
        data.append(votes_for)
    table = pd.DataFrame(
        data,
        columns=['sr', 'ldpr', 'pr', 'kprf', 'apple', 'er', 'pravo']
    )
    view = table.div(table.sum(axis=1), axis=0)
    order = view.sum(axis=0)
    order = order.sort_values(ascending=False).index
    view = view.reindex(columns=order)
    view.plot(kind='box')


def show_votes_share_party_share_corellation(uiks):
    data = []
    for uik in uiks:
        stats = uik.stats
        data.append((stats.total, stats.votes) + stats.votes_for)
    table = pd.DataFrame(
        data,
        columns=['total', 'votes', 'sr', 'ldpr', 'pr', 'kprf', 'apple', 'er', 'pravo']
    )
    share = table.votes / table.total
    er = table.er / table.votes
    kprf = table.kprf / table.votes
    ldpr = table.ldpr / table.votes
    fig, ax = plt.subplots()
    ax.scatter(share, er, color=BLUE, s=1, alpha=0.1)
    ax.scatter(share, kprf, color=RED, s=1, alpha=0.1)
    ax.scatter(share, ldpr, color=PURPLE, s=1, alpha=0.05)
    ax.scatter([], [], color=BLUE, label=u'ЕР')
    ax.scatter([], [], color=RED, label=u'КПРФ')
    ax.scatter([], [], color=PURPLE, label=u'ЛДПР')
    ax.set_xlim(0.2, 1.05)
    ax.set_ylim(-0.1, 1.1)
    formater = mtick.FuncFormatter(
        lambda value, _: '{}%'.format(int(value * 100))
    )
    ax.xaxis.set_major_formatter(formater)
    ax.yaxis.set_major_formatter(formater)
    ax.set_xlabel(u'Явка')
    ax.set_ylabel(u'Доля голосов')
    ax.legend(loc='upper left', markerscale=1.5)
    fig.savefig('fig.png', dpi=300, bbox_inches='tight')


def show_votes_share_total_corellation(uiks):
    data = []
    for uik in uiks:
        stats = uik.stats
        total = stats.size
        votes = stats.votes
        data.append((votes, total))
    table = pd.DataFrame(data, columns=['votes', 'total'])
    share = table.votes / table.total
    fig, ax = plt.subplots()
    ax.scatter(share, table.total, color=BLUE, s=2, alpha=0.1)
    ax.set_xlim(0.2, 1.05)
    ax.set_ylim(-100, 3300)
    formater = mtick.FuncFormatter(
        lambda value, _: '{}%'.format(int(value * 100))
    )
    ax.xaxis.set_major_formatter(formater)
    ax.set_xlabel(u'Явка')
    ax.set_ylabel(u'Число участков')


def dump_map_data(uiks):
    data = []
    for uik in uiks:
        if uik.address and uik.address.coordinates:
            coordinates = uik.address.coordinates
            stats = uik.stats
            votes = stats.votes
            if votes:
                total = stats.total
                share = float(votes) / total
                votes_for = stats.votes_for
                er = float(votes_for.er) / votes
                kprf = float(votes_for.kprf) / votes
                ldpr = float(votes_for.ldpr) / votes
                apple = float(votes_for.apple) / votes
                data.append((
                    uik.region, coordinates.longitude, coordinates.latitude,
                    total, votes, share, er, kprf, ldpr, apple
                ))
    table = pd.DataFrame(
        data,
        columns=['region', 'longitude', 'latitude',
                 'total', 'votes', 'share', 'er', 'kprf', 'ldpr', 'apple']
    )
    table.to_csv(MAP_DATA, index=False)


def dump_data(uiks):
    data = []
    for uik in uiks:
        address = uik.address
        if address:
            coordinates = address.coordinates
            if coordinates:
                longitude, latitude = coordinates
                stats = uik.stats
                size, total, votes, votes_for = stats
                sr, ldpr, pr, kprf, apple, er, pravo = votes_for
                data.append((
                    uik.region, longitude, latitude,
                    size, total, votes, sr, ldpr, pr,
                    kprf, apple, er, pravo
                ))
    table = pd.DataFrame(
        data,
        columns=['region', 'longitude', 'latitude',
                 'size', 'total', 'votes', 'sr',
                 'ldpr', 'pr', 'kprf', 'apple', 'er', 'pravo']
    )
    table.to_csv(DATA, index=False)
