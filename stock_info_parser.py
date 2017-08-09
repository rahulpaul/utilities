import sys
import json
import traceback
import requests

from datetime import date
from collections import defaultdict
from bs4 import BeautifulSoup
from peewee import Model, CharField, IntegerField, DoubleField, DateField, SqliteDatabase


BASE_URL = 'http://www.moneycontrol.com/india/stockmarket/sector-classification/marketstatistics/nse'


# sectors = ('automotive', 'banking-finance', 'cement-construction', 'chemicals', 'conglomerates', 'consumer-durable', 'consumer-non-durable',
#            'engineering', 'food-beverage', 'gold-etf', 'technology', 'manufacturing', 'media', 'metals-mining', 'oil-gas', 'pharmaceuticals',
#            'retail-real-estate', 'services', 'telecom', 'tobacco', 'utilities', 'miscellaneous')

sectors = ('automotive', )


MONTHS = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
EMPTY_VALUES = ('', '-', '*-', '_')


db = SqliteDatabase('stocks.db')


class BaseModel(Model):

    def __str__(self):
        r = {}
        for k in self._data.keys():
            try:
                r[k] = str(getattr(self, k))
            except:
                r[k] = json.dumps(getattr(self, k))
        return str(r)


class CompanyInfo(BaseModel):

    company_name = CharField()
    symbol = CharField(null=True)
    isin = CharField(null=True)
    sector = CharField()
    industry = CharField()
    market_cap = IntegerField()
    mc_url = CharField()

    class Meta:
        database = db
        db_table = 'company_info'


class FinancialInfo(BaseModel):

    symbol = CharField()
    trade_date = DateField()
    datatype = CharField()  # standalone / consolidated
    market_cap = DoubleField(null=True)
    pe_ratio = DoubleField(null=True)
    book_value = DoubleField(null=True)
    dividend_pct = DoubleField(null=True)
    industry_pe_ratio = DoubleField(null=True)
    eps = DoubleField(null=True)
    pc_ratio = DoubleField(null=True)
    pb_ratio = DoubleField(null=True)
    div_yield_pct = DoubleField(null=True)
    face_value = DoubleField(null=True)
    deliverable_pct = DoubleField(null=True)

    class Meta:
        database = db
        db_table = 'financial_info'


class IncomeStatement(BaseModel):

    month_of_year = IntegerField()
    year = IntegerField()
    month_year_str = CharField()
    net_sales = DoubleField()
    other_income = DoubleField()
    pbdit = DoubleField()
    net_profit = DoubleField()

    class Meta:
        database = db
        db_table = 'income_statement'


class BalanceSheet(BaseModel):

    month = CharField()
    year = IntegerField()
    month_year_str = CharField()
    total_share_capital = DoubleField()
    net_worth = DoubleField()
    total_debt = DoubleField()
    net_block = DoubleField()
    investments = DoubleField()
    total_assets = DoubleField()

    class Meta:
        database = db
        db_table = 'balance_sheet'


def create_datasource(sector):
    url = BASE_URL + '/' + sector + '.html'
    print('Parsing url: ', url)
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content, 'html.parser')
    table = soup.find('table', class_='tbldata14 bdrtpg')
    rows = table.find_all('tr')
    rowsiter = iter(rows)
    headers = [header.text for header in next(rowsiter).find_all('th')]  # first row is the header
    datasource = []
    for row in rowsiter:
        columns = row.find_all('td')
        company_name, industry, _, _, _, market_cap = columns
        mc_url = 'http://moneycontrol.com' + company_name.find('a')['href']
        mapping = {'company_name': company_name.text, 'industry': industry.text, 'sector': sector, 'market_cap': int(market_cap.text.replace(',', '')), 'mc_url': mc_url}
        datasource.append(mapping)
    return datasource


def populate_company_info():
    combined_datasource = []
    for sector in sectors:
        try:
            combined_datasource += create_datasource(sector)
        except Exception as ex:
            print('Error processing sector = ', sector, 'exception = ', ex)
    with db.atomic():
        for idx in range(0, len(combined_datasource), 100):
            CompanyInfo.insert_many(combined_datasource[idx: idx + 100]).execute()


def parse_company_page(company_info):
    global MONTHS

    print('Parsing url: ', company_info.mc_url)
    resp = requests.get(company_info.mc_url)
    soup = BeautifulSoup(resp.content, 'html.parser')

    div = soup.find('div', {'class': 'FL gry10'})
    symbol, isin = [x.split(':')[1].strip() for x in div.text.split('|')[1: 3]]

    print(symbol, isin)

    # update symbol and isin to db
    company_info.symbol, company_info.isin = symbol, isin
    company_info.save()

    type_dict = {'standalone': 'mktdet_1', 'consolidated': 'mktdet_2'}

    def _create_fin_info(datatype):
        mktdata = soup.find('div', {'id': type_dict[datatype]})
        children = mktdata.findChildren(recursive=False)

        values = children[0].find_all('div', {'class': 'FR gD_12'})
        values = [None if value.text.strip() in EMPTY_VALUES else float(value.text.strip().replace(',', '').replace('%', '')) for value in values]
        mkt_cap, pe_ratio, book_value, div_pct, mkt_lot, industry_pe_ratio = values

        values = children[1].find_all('div', {'class': 'FR gD_12'})
        values = [None if value.text.strip() in EMPTY_VALUES else float(value.text.strip().replace(',', '').replace('%', '')) for value in values]
        eps, pc_ratio, pb_ratio, div_yield_pct, face_value = values

        fin_info = FinancialInfo(symbol=symbol, trade_date=date.today(), datatype=datatype,
                                 market_cap=mkt_cap, pe_ratio=pe_ratio, book_value=book_value,
                                 dividend_pct=div_pct, industry_pe_ratio=industry_pe_ratio, eps=eps,
                                 pc_ratio=pc_ratio, pb_ratio=pb_ratio, div_yield_pct=div_yield_pct, face_value=face_value)

        with open('fin-info.dat', mode='wb') as file:
            import pickle
            pickle.dump(fin_info, file)

        # insert to db
        fin_info.save()

    _create_fin_info('standalone')
    _create_fin_info('consolidated')

    div = soup.find('div', {'id': 'findet_1'})
    table = div.find('table')
    rows = table.find_all('tr')
    row_iter = iter(rows)

    month_years = [x.text for x in next(row_iter).find_all('td')][1:-1]
    month_year_tuples = [(MONTHS.index(month) + 1, 2000 + int(year)) for month, year in [month_year.split("'") for month_year in month_years]]
    months = [month for month, _ in month_year_tuples]
    years = [year for _, year in month_year_tuples]

    cols = ['net_sales', 'other_income', 'pbdit', 'net_profit']
    col_values = defaultdict(list)
    for col, row in zip(cols, row_iter):
        col_values[col].extend([None if x.text.strip() in EMPTY_VALUES else float(x.text.strip().replace(',', '')) for x in row.find_all('td')[1:-1]])

    for x1, x2, x3, x4, x5, x6, x7 in zip(months, years, month_years, col_values['net_sales'], col_values['other_income'], col_values['pbdit'], col_values['net_profit']):
        income_stmt = IncomeStatement(month_of_year=x1, year=x2, month_year_str=x3, net_sales=x4, other_income=x5, pbdit=x6, net_profit=x7)
        income_stmt.save()


def main():
    try:
        db.connect()
        db.create_tables([CompanyInfo, FinancialInfo, IncomeStatement, BalanceSheet], safe=True)

        company_list = CompanyInfo.select()
        if not company_list:
            populate_company_info()
            company_list = CompanyInfo.select()

        for idx, company in enumerate(company_list):
            print('Processing entry no. ', idx + 1)
            try:
                parse_company_page(company)
            except Exception as ex:
                print('Error parsing company page for company = ', company.company_name, ', error = ', ex)
                traceback.print_exc(file=sys.stderr)

    finally:
        db.close()


if __name__ == '__main__':
    main()
