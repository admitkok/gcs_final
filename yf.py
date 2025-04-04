import yfinance as yf
dat = yf.Ticker("MSFT")

dat.info
print(dat.info)
dat.calendar
print(dat.calendar)
dat.analyst_price_targets
print(dat.analyst_price_targets)
dat.quarterly_income_stmt
print(dat.quarterly_income_stmt)
dat.history(period='1mo')
print(dat.history(period='1mo'))
dat.option_chain(dat.options[0]).calls
print(dat.option_chain(dat.options[0]).calls)