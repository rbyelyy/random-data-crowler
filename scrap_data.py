#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import logging
import os
import pypyodbc
import re
import ssl
import eventlet
import pandas.io.sql

from beautifulscraper import BeautifulScraper
from bs4 import BeautifulSoup

eventlet.monkey_patch()


class ScrapData(object):
    def __init__(self):
        self.scraper = BeautifulScraper()
        self.content = []
        self.list_of_parsed_values = []
        self.logger = logging.getLogger('data_scrap')
        self.logger.setLevel(logging.DEBUG)
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger.addHandler(self.ch)

    def factory(type):
        if type == "Florida": return Florida()
        if type == "Illinoise": return Illinoise()
        if type == "MaineGov": return MaineGov()
        if type == "Michigan": return Michigan()
        assert 0, "Bad shape creation: " + type

    factory = staticmethod(factory)

    @staticmethod
    def remove_new_lines(l):
        """
        Remove all none numeric and alphabetic chars.
        :param l:
        """
        return [i.strip(' \t\n\r') for i in l]

    def get_default_page(self, url):
        try:
            with eventlet.Timeout(10):
                self.logger.debug("GET request is send to site: {0}".format(url))

                if hasattr(ssl, '_create_unverified_context'):
                    ssl._create_default_https_context = ssl._create_unverified_context

                body = self.scraper.go(url)
                return body
        except eventlet.timeout.Timeout:
            self.logger.error("Timeout error occurs. Execution of the app will be finished.")
            exit("Timeout {0} occurs while opening {1}. May be url is not reachable now? Try again.".format(15, url))

    def write_to_csv(self, file_name, titles):
        try:
            self.logger.info("Start creating csv file: {0} ...".format(file_name))
            with open(file_name + '.csv', "w+") as f:
                csv.register_dialect("custom", delimiter=",", skipinitialspace=True)
                writer = csv.writer(f, dialect="custom")
                writer.writerows([titles])
                for tup in self.list_of_parsed_values:
                    writer.writerow(tup)
            self.logger.info("File '{0}' is created.".format(str(self.__class__.__name__).lower()))
        except:
            # TODO: Define exception
            self.logger.error("{0} cannot be created. Execution stopped.".format(file_name))
            exit("{0} cannot be created. Execution stopped.".format(file_name))

    def remove_all_csv(self):
        self.logger.info("Deleting {1} files in directory: {0}".format(os.path.dirname(os.path.realpath(__file__)),
                                                                       str(self.__class__.__name__).lower()))
        try:
            os.remove(str(self.__class__.__name__).lower() + '.csv')
        except OSError:
            self.logger.info("Deleting {1} files in directory: {0} was NOT successful"
                             .format(os.path.dirname(os.path.realpath(__file__))),
                             str(self.__class__.__name__).lower() + '.csv')
            exit("Execution stopped because file were not deleted properly.")


class Michigan(ScrapData):
    def __init__(self):
        ScrapData.__init__(self)
        self.url = "https://www.buy4michigan.com/bso/external/publicBids.sdo"
        self.default_page = self.get_default_page(self.url)
        self.number_of_page = int(self.get_number_of_page())
        self.column_title = ['Bid #', 'Alternate Id', 'Buyer', 'Description', 'Purchase Method', 'Bid Opening Date',
                             'Bid Holder List']
        self.bid = []
        self.alternate = []
        self.buyer = []
        self.description = []
        self.method = []
        self.date = []

    def get_number_of_page(self):
        try:
            result = self.default_page.find("td", attrs={"class": "inputs-01", "align": "center", "valign": "bottom"}). \
                findChildren(text=True)
            f = [j for j in [re.sub('[^0-9]', '', i) for i in result] if j]
            return max(f)
        except AttributeError:
            exit("Cannot define number of pages on the start page. Please try once more.")

    def get_all_page_content(self):
        for i in xrange(self.number_of_page):
            post_data = "status=Open&mode=navigation&currentPage={0}&querySql=%5BPROCESSED%5DEMITwuQtpq_" \
                        "7mcLx2l8vez9KFdhQmaUKvl3KRI3bBnjh1wpI5LCe9RQldirRXnlbBnAJAAbMLA2OeENTUazHafpdWxLBhi" \
                        "7Tpok8GumyeBBZ7s-CBhgP9wE_A5W1OzSEfdWbooNezU2amSL6lihHpsUEqfp3AvjTJ-UtUFACbNDYHUwmajUI" \
                        "fXBsXq6ZjmgzIGI2UlUtaJa95XdpGb0PDZ9sx0QggLKj59-g4q5RSpSq0guYebnHjb_xaBjoHg9ZXHfMdpiSN5Nt-" \
                        "jO-r0zJwHaN8P1DxH0HrL29YDExEWeRO6-2twjSNujVvdZqBj0AuEJtU5IqXspWcVaE4DoEpG-_ywp3p96ZAx7mS7LF" \
                        "JtrEw-vmEp65idk4cAL71ogWglcSIDRPEf5IqZJNyvpRAQ5JsXogiUGRtJkPcro3EETQMfJTp41xKMX1YgHNIJAhTP" \
                        "MzGBh-BQrmUn8lsZ9RE6y5oauRY9awAX1uKAk3ZrfXWP765XM24TJdocjmZm4qOgI292qLGx6eIUVAkeAT-4oNYFe" \
                        "ozroT0kqeAZL2qcbPkcOp68D-DRly8ENMlKEnP5QgZIMY15ziv4lKMgB9LhEzSyZxeib_VWrEHWQJRrsOGDRuJxR" \
                        "O2T69Ua4zxdxH9t4VLjxU1wjQvf0xX4adRoZnF_Dlb3vUMUullcMcedvi7iWnRwaurgCSVtwj72aCTGEoUfyGTS" \
                        "lQ7OkSClq7esJGb0bywPbZYa7oT1QFCPrmm0klNIZQ8Gx_aluIO3D8g_d5xByjodgQjD3e1eyPRUesOyUSfmVX" \
                        "_-aCf--JfSVpRsCF3neb6dUI6v0261mqDesSKvTHeRxNnvZIC5qWx3GtmdAvxs3sx8Rh6Uw5ioJOtZV0zaVuk" \
                        "Wk4CIfsOQCvvxOTtWx16I5wSlPtJduMJ3kfO9brciXF2xK7EZzb4YFQdUlK3-WMXNjB1V1tEu7rrrObKsSfuy" \
                        "UP_cNT4ae1s4ZxZpQIGkTKqchkK7E7vA8S9_UfkF47u2pXC36NK6AwzUhykEJ9Tsl_RPKQ73Kb5ME8hp_5jTj" \
                        "zVJxD-Tv7TJ00LIGwRyz5o0cEnq7t-kkXnunSalZ3EGGQ4ZA2e9sNWoxqO0D7BdQ58Vm1Cz7ASul44xhwxYTjs" \
                        "2uqEyKsUtayABWeFLjf1oz-W--57P4RyfArLBfJqeODJlbEwocQyk6qUG78989PNOZYq6kx-maMFIpvrxxCtm2k" \
                        "uJn2GuXEvQxfacppN-P29BGTdEo4ZYal-yTNOTaQPm-qfiFYOxLoY-WzugNZTYqtAOhs8gEgcS668thwQTTY-Q" \
                        "19z8M5K2CbB-E76dq7cB5pKvYdRycehxZFC5B-vZOc6BoKpkdjTiJsS0W1yidmRbuLl6kNmr3Vg1KSbTAOrJmZ" \
                        "who9xIQBjIuRjbzyIb-z3mo3-TBShMU_Mid5L90XB655XLdu9RsFmdshqEu3_LH5bU7mWdUh2TGFk6wzy0" \
                        "PXpZEzO-TISs0c93ddgMdu6fVxhXpP3NCTaNcoWvUfTxPJaCEO7AlVMTscZpp-ui8iH57SWWojf3hmtnvj" \
                        "0sskI48K-8cQWtoO63VEePQAY4IzAw1S8oYRGQlfyQggdh58T-JuDz5efUnOZR70ErAdCE4tgVar6YGcN" \
                        "eOtDzjY9I3cu3EigUzB_966ULvdsYjGQoF3CfysZp_UYyvo8It51a62paG1AOwUgoFaVnLkDXZFFeg3XK" \
                        "GmLVruTJHcLz3TMD4af7Vk3877ZenEVmWdr79a8MKE8b-qtXJREOwFSmn4NtqzSL1uakmtMRD_MTetw0z" \
                        "6GjqisHPpd3vBb9mw_fjeZV4c2pYwiduZHNwbpiwikdq9WyVLdKTsMps7GNoJNIcwwmzMcQofptjV7IKg" \
                        "62ltxBXBLftvIL0f-O5BO4KRpDQ2rGdpkrMT1k-gnoCt0OWbiNVUcPVAJxIadP7vc15qZXAdZ5eW_ibeE" \
                        "1wK2Cm_r4tYuwE1gSV1_J-tHCHdzOkQUVtuA3kq3EwtShSJLX7iM4IoGlVa9RAixpT-8Orq7aKG8yxHdR" \
                        "0q__ozahwqnkgdmLJ3HsE3ZSuFY9USQAsa-r5BadciaPfszWwyVXil6RW5qK25m4p72OElWNsTN0VK45Q" \
                        "1ntam_tAeDsDKyT02x07WePg0nogG9ADlU5vNaVPKAWt6W_jOyjamvXKqYdWeQ8rdYtT0-wfJzUyqUJjcy" \
                        "h0mQtBTKidUiSbbzCbsGsaOQhn_v_rJEQ0mEMKPmN_0so1nZxlFY8efIhrakcoIWzHba5f5HMMBVRGk" \
                        "J6EhMzuA_y_96UOvKnDo382QCR7XBKhR8wdwRHjpGz5grtVW&sortBy=&sortByIndex=0&sortByDe" \
                        "scending=false&viewAllFlag=true&activeResult=Normal+Bid&category=All".format(i + 1)

            if hasattr(ssl, '_create_unverified_context'):
                ssl._create_default_https_context = ssl._create_unverified_context

            self.logger.info("POST request to {0} page {1}".format(self.url, i + 1))
            body = self.scraper.go(self.url, data=post_data)
            self.content.append(body)

    def data_parse(self):
        parsed_data = []
        for data in self.content:
            table_tag = BeautifulSoup(str(
                data.findAll('table', attrs={"name": "resultsTable"})))

            for line in table_tag.findAll('tr'):
                line_text = line.get_text()
                if 'Purchase Method' not in line_text:
                    if '\n\n' in line_text[-2:]:
                        line_text += 'None'
                    cleaned_elements = [i.replace('\t', "").replace('\n', "").strip() for i in
                                        line_text.strip().replace("\n\n\n\n\n", 'None\n').split('\n')]
                    remove_empty_element = [i for i in cleaned_elements if len(i) > 1]
                    parsed_data.append(remove_empty_element)
                    self.list_of_parsed_values = parsed_data[1:]


class MaineGov(ScrapData):
    def __init__(self):
        ScrapData.__init__(self)
        self.url = "http://www.maine.gov/purchases/venbid/rfp.shtml"
        self.default_page = self.get_default_page(self.url)
        self.number_of_page = 1
        self.column_title = ['RFP Title', 'RFP #', 'Issuing Department ', 'Proposal Due Date', 'Date Posted']

    def get_all_page_content(self):
        for i in xrange(self.number_of_page):
            body = self.scraper.go(self.url)
            self.content.append(body)

    def data_parse(self):
        div_text_table_title = []

        for data in self.content:
            tabple_tag = BeautifulSoup(str(
                data.findAll('table', attrs={"summary": "This table contains a list of world trade opportunities."})))
            for link in tabple_tag.select('tr div'):
                div_text_table_title.append(link.getText().replace("'", ""))
                # TODO: Add global function for removing redundant chars

            div_text_table = div_text_table_title[5:]  # remove row with titles

            list_strip = lambda lst, sz: (lst[_:_ + sz] for _ in range(0, len(lst), sz))

            self.list_of_parsed_values = list(list_strip(div_text_table, 5))


class Florida(ScrapData):
    def __init__(self):
        ScrapData.__init__(self)
        self.url = "http://www.myflorida.com/apps/vbs/!vbs_www.search_r2.matching_ads"
        self.default_page = self.get_default_page(self.url)
        self.number_of_page = 1
        self.column_title = ['Title', 'Number', 'Version', 'Ad Type', 'Begin', 'End']
        self.Title = []
        self.Version = []
        self.Number = []
        self.Ad_Type = []
        self.Begin = []
        self.End = []

    def get_all_page_content(self):
        for i in xrange(self.number_of_page):
            self.logger.info("GET reques to {0}".format(self.url))
            body = self.scraper.go(self.url)
            self.content.append(body)

    def data_parse(self):

        for data in self.content:
            table_tag = BeautifulSoup(str(data.findAll('table', attrs={'border': '1'})))
            # d = table_tag.findAll('tr')
            a_tags = BeautifulSoup(str(table_tag.findAll('tr'))).findAll('a', text=True)
            for a in a_tags:
                a_text = BeautifulSoup(str(a))
                self.Number.append(a_text.getText().encode('utf-8'))

            for title in table_tag.select('tr > td:nth-of-type(1)'):
                self.Title.append(
                    title.getText().strip().replace('\n', ' ').replace('\r', '').replace('\t', '').encode('utf-8')
                )

            for version in table_tag.select('tr > td:nth-of-type(3)'):
                self.Version.append(version.getText().encode('utf-8'))

            for ad_type in table_tag.select('tr > td:nth-of-type(4)'):
                self.Ad_Type.append(ad_type.getText().encode('utf-8'))

            for begin in table_tag.select('tr > td:nth-of-type(5)'):
                self.Begin.append(begin.getText().encode('utf-8'))

            for end in table_tag.select('tr > td:nth-of-type(6)'):
                self.End.append(end.getText().encode('utf-8'))

            self.list_of_parsed_values = list(zip(
                self.Title,
                self.Number,
                self.Version,
                self.Ad_Type,
                self.Begin,
                self.End)
            )


class Illinoise(ScrapData):
    def __init__(self):
        ScrapData.__init__(self)
        self.url = "http://www.purchase.state.il.us/ipb/IllinoisBID.nsf/" \
                   "viewsolicitationsopenbydate?openview&start=1&count=999?OpenView"
        self.default_page = self.get_default_page(self.url)
        self.column_title = ['Reference #', 'Title', 'Due Date']
        self.number_of_page = 1
        self.Reference = []
        self.Title = []
        self.Due_Date = []

    def get_default_page(self, url):
        return self.scraper.go(url)

    def get_all_page_content(self):
        for i in xrange(self.number_of_page):
            body = self.scraper.go(self.url)
            self.content.append(body)

    def data_parse(self):

        for data in self.content:
            table_tag = BeautifulSoup(str(data.findAll('table', attrs={"border": "0", "cellpadding": "2"})))

            a_tags = BeautifulSoup(str(table_tag.findAll('tr'))).findAll('a', text=True)
            for a in a_tags:
                a_text = BeautifulSoup(str(a))
                self.Reference.append(a_text.getText().encode('utf-8'))

            for title in table_tag.select('tr > td:nth-of-type(3)'):
                self.Title.append(
                    title.getText().strip().replace('\n', ' ').replace('\r', '').replace('\t', '').encode('utf-8')
                )

            for version in table_tag.select('tr > td:nth-of-type(5)'):
                self.Due_Date.append(version.getText().encode('utf-8'))

            self.list_of_parsed_values = list(zip(
                self.Reference, self.Title, self.Due_Date))


class MSDatabase(object):
    def __init__(self):
        self.db_name = 'data_scrap.MDB'
        self.path_to_mdb_file = os.path.dirname(os.path.abspath(__file__)) + '\\' + self.db_name
        self.connection = self.create_connection()

    def create_connection(self):
        try:
            connection_string = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=' + self.path_to_mdb_file
            connection_mdb = pypyodbc.connect(connection_string)
            return connection_mdb
        except pypyodbc.Error:
            exit("Connection to file {0} cannot be established".format(self.path_to_mdb_file))

    def fetch_data_from_table(self, table):
        try:
            df = pandas.io.sql.read_frame("""SELECT * from """ + table + """;""", self.connection)
            return df
        except:
            # TODO: Define exception type
            exit("Cannot fetch from table. SELECT * from " + table)

    def compare_data(self, unique_value, table, column):
        old = self.fetch_data_from_table(table=table)
        if unique_value in list(old[column]):
            return True
        else:
            return False

    def insert_row_into_table(self, row, table):
        SQL = "INSERT INTO " + table + " values " + row + ";"
        try:
            self.connection.cursor().execute(SQL).commit()
        except:
            # TODO: Add define exception
            exit("Cannot insert {0} into table {1}".format(SQL, table))

    def push_data_into_table(self, csv_file, table, unique_column):
        data_df = pandas.read_csv(csv_file)
        for i in data_df.itertuples():
            row = str(tuple((str(e) for e in i[1:])))

            if table == 'illinoise' or 'michigan':
                unique_value = str(i[1])
            elif table == 'florida' or 'main_gov':
                unique_value = str(i[2])
            else:
                unique_value = str(i[1])

            if not self.compare_data(unique_value, table, unique_column):
                self.insert_row_into_table(row, table=table)


if __name__ == '__main__':

    def site_object_gen():
        types = ScrapData.__subclasses__()
        return types

    site_object_list = site_object_gen()

    sites = [_ for _ in site_object_list]
    for s in sites:
        s = ScrapData.factory(s.__name__)
        s.get_all_page_content()
        s.data_parse()
        csv_file_name = str(s.__class__.__name__).lower()
        s.write_to_csv(csv_file_name, s.column_title)

    mdb = MSDatabase()
    connection = mdb.create_connection()
    mdb.push_data_into_table('michigan.csv', 'michigan', 'bid #')
    mdb.push_data_into_table('illinoise.csv', 'illinoise', 'reference')
    mdb.push_data_into_table('myflorida.csv', 'florida', 'numbers')
    mdb.push_data_into_table('mainegov.csv', 'main_gov', 'rfp')
    connection.close()

    for s in sites:
        s = ScrapData.factory(s.__name__)
        s.remove_all_csv()
