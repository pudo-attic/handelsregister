import os
import time
import logging
import requests
import dataset
from lxml import html
from itertools import count
# from thready import threaded

log = logging.getLogger(__name__)

db = os.environ.get('DATABASE_URI', 'sqlite:///data.sqlite')
engine = dataset.connect(db)
companies = engine['de_handelsregister']

logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

PAGE_SIZE = 100
SEARCH_URL = 'https://www.handelsregister.de/rp_web/mask.do?Typ=e'
PAGE_URL = 'https://www.handelsregister.de/rp_web/result.do?Page=%s'
DOC_URL = 'https://www.handelsregister.de/rp_web/document.do?doctyp=UT&index=%s'

STATES = ["NW", "BW", "BY", "BE", "BR", "HB", "HH", "HE",
          "MV", "NI", "RP", "SL", "SN", "ST", "SH", "TH"]

QUERY = {
    "suchTyp": "e",
    "registerArt": "",
    "registerNummer": "1",
    "registergericht": "",
    "schlagwoerter": "",
    "schlagwortOptionen": "2",
    "niederlassung": "",
    "rechtsform": "",
    "postleitzahl": "",
    "ort": "",
    "strasse": "",
    "suchOptionenGeloescht": "true",
    "ergebnisseProSeite": PAGE_SIZE,
    "btnSuche": "Suchen"
}


def scrape_states():
    for state in STATES:
        q = QUERY.copy()
        q['bundesland' + state] = "on"
        scrape_state(q, state)


def scrape_state(q, state):
    failed_state = 0
    for i in count(1):
        if failed_state > 1000:
            time.sleep(10)
            return
        exist = list(companies.find(state=state, number=i))
        if len(exist) and len(exist) == exist[0]['result_count']:
            continue

        q["registerNummer"] = i
        sess = requests.Session()
        res = sess.post(SEARCH_URL, data=q)
        results = 0
        page = 1
        while True:
            total, page_results = parse_results(sess, state, i, res.content,
                                                page)
            results += page_results
            if total == -1:
                failed_state += 1
                break
            else:
                failed_state = 0
            # print 'PAGE', total, page, page_results, results
            if results >= total:
                break
            page += 1
            res = sess.get(PAGE_URL % page)


def parse_results(sess, state, i, page_html, page):
    doc = html.fromstring(page_html)
    content = doc.find('.//div[@id="inhalt"]')

    count_text = content.findtext('./p')
    if not count_text or 'Ihre Suche hat' not in count_text:
        return -1, 0
    _, count_text = count_text.split('hat', 1)
    count_text, _ = count_text.split('Treffer', 1)
    results = int(count_text)
    log.info('Register %s (#%s): %s results, page: %s',
             state, i, results, page)

    current_html = ''
    current_index = 0
    for tr in content.findall('./table[@class="RegPortErg"]/tr'):
        tds = tr.findall('./td')
        if len(tds) == 1 and tds[0].get('class') == 'RegPortErg_AZ':
            if len(current_html):
                current_html = '<table>%s</table>' % current_html
                scrape_ut(sess, state, results, i, current_html,
                          current_index, page)
                current_index += 1
                current_html = ''
        current_html += html.tostring(tr)
    return results, current_index


def scrape_ut(sess, state, results, i, index_html, index, page):
    if companies.find_one(state=state, number=i,
                          result=index, result_page=page):
        return
    try:
        doc = html.fromstring(index_html)
        title = doc.findtext('.//td[@class="RegPortErg_FirmaKopf"]')

        res = sess.get(DOC_URL % index)
        doc = html.fromstring(res.content)
        content = doc.find('.//div[@id="inhalt"]')
        if content is None:
            return
        ut_html = html.tostring(content)
        log.info("UT (%s/%s): %s", state, i, title)
        data = {
            'state': state,
            'number': i,
            'result': index,
            'title': title,
            'result_page': page,
            'result_count': results,
            'result_html': index_html.encode('utf-8'),
            'ut_html': ut_html.encode('utf-8')
        }
        companies.upsert(data, ['state', 'number', 'result', 'result_page'])
    except Exception, e:
        print e
        time.sleep(5)


if __name__ == '__main__':
    scrape_states()
