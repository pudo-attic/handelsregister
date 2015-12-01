import os
import time
import requests
import dataset
from lxml import html
from itertools import count
# from thready import threaded

db = os.environ.get('DATABASE_URI', 'sqlite:///data.sqlite')
engine = dataset.connect(db)
companies = engine['de_handelsregister']

SEARCH_URL = 'https://www.handelsregister.de/rp_web/mask.do?Typ=e'
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
    "ergebnisseProSeite": "100",
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
            return
        time.sleep(1.0)
        exist = list(companies.find(state=state, number=i))
        if len(exist) and len(exist) == exist[0]['result_count']:
            continue

        q["registerNummer"] = i
        sess = requests.Session()
        res = sess.post(SEARCH_URL, data=q)
        doc = html.fromstring(res.content)
        content = doc.find('.//div[@id="inhalt"]')

        count_text = content.findtext('./p')
        if not count_text or 'Ihre Suche hat' not in count_text:
            failed_state += 1
            continue
        _, count_text = count_text.split('hat', 1)
        count_text, _ = count_text.split('Treffer', 1)
        results = int(count_text)
        print 'Reg %s (%s): %s' % (state, i, results)
        failed_state = 0

        current_html = ''
        current_index = 0
        for tr in content.findall('./table[@class="RegPortErg"]/tr'):
            tds = tr.findall('./td')
            if len(tds) == 1 and tds[0].get('class') == 'RegPortErg_AZ':
                if len(current_html):
                    current_html = '<table>%s</table>' % current_html
                    scrape_ut(sess, state, results, i, current_html,
                              current_index)
                    current_index += 1
                    current_html = ''
            current_html += html.tostring(tr)


def scrape_ut(sess, state, results, i, index_html, index):
    res = sess.get(DOC_URL % index)
    doc = html.fromstring(res.content)
    content = doc.find('.//div[@id="inhalt"]')
    ut_html = html.tostring(content)
    data = {
        'state': state,
        'number': i,
        'result': index,
        'result_count': results,
        'result_html': index_html,
        'ut_html': ut_html
    }
    companies.upsert(data, ['state', 'number', 'result'])


if __name__ == '__main__':
    scrape_states()
