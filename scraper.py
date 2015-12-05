import os
import time
import logging
import requests
import dataset
from lxml import html
from itertools import count
from thready import threaded

log = logging.getLogger(__name__)

db = os.environ.get('DATABASE_URI', 'sqlite:///data.sqlite')
engine = dataset.connect(db)
companies = engine['de_handelsregister']

logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

PAGE_SIZE = 10
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
    def states():
        for state in STATES:
            q = QUERY.copy()
            q['bundesland' + state] = "on"
            yield (q, state)
    # for s in states():
    #     scrape_state(s)
    threaded(states(), scrape_state, num_threads=5)


def scrape_state(arg):
    q, state = arg
    failed_state = 0
    for i in count(1):
        if failed_state > 1000:
            time.sleep(10)
            return
        exist = list(companies.find(state=state, number=i))
        if len(exist) and len(exist) == exist[0]['result_count']:
            continue

        q["registerNummer"] = i
        try:
            sess = requests.Session()
            res = sess.post(SEARCH_URL, data=q)
            if '403.html' in res.url:
                raise ValueError()
        except Exception, e:
            log.exception(e)
            time.sleep(10)
            continue

        results = 0
        page = 1
        page_results = 0
        while True:
            total, page_results = parse_results(sess, state, i, res.content,
                                                page, page_results)
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
            page_results = page_results + 1
            try:
                res = sess.get(PAGE_URL % page)
                if '403.html' in res.url:
                    raise ValueError()
            except Exception, e:
                log.exception(e)
                time.sleep(10)
                break


def parse_results(sess, state, i, page_html, page, current_index):
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

    current_html = None

    for tr in content.findall('./table[@class="RegPortErg"]/tr'):
        tds = tr.findall('./td')
        if len(tds) == 1 and tds[0].get('class') == 'RegPortErg_AZ':
            if current_html and len(current_html):
                chtml = '<table>%s</table>' % current_html
                scrape_ut(sess, state, results, i, chtml,
                          current_index, page)
                current_index += 1

            current_html = ''

        if current_html is not None:
            current_html += html.tostring(tr)

    if current_html and len(current_html):
        chtml = '<table>%s</table>' % current_html
        scrape_ut(sess, state, results, i, chtml,
                  current_index, page)

    return results, current_index


def scrape_ut(sess, state, results, i, index_html, index, page):
    # print results, i, index, page
    if companies.find_one(state=state, number=i,
                          result=index, result_page=page):
        return
    try:
        doc = html.fromstring(index_html)
        title = doc.findtext('.//td[@class="RegPortErg_FirmaKopf"]')
        res = sess.get(DOC_URL % index)
        if '403.html' in res.url:
            raise ValueError()
        doc = html.fromstring(res.content)
        content = doc.find('.//div[@id="inhalt"]')
        # ut_title = content.find('.//td//b')
        # if ut_title is not None:
        #     ut_title = ut_title.tail.strip()
        if content is None:
            return
        ut_html = html.tostring(content)
        log.info("UT (%s/%s-%s): %s", state, i, index, title)
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
        log.exception(e)
        time.sleep(5)


if __name__ == '__main__':
    scrape_states()
