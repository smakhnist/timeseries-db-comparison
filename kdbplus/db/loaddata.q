DAYS:100
TICKS_PER_DAY:10000
SYMBOLS:`AAPL`MSFT`GOOG

// create columns for the trades table
dates:{[i] 2025.01.01+(floor i%TICKS_PER_DAY)} til (DAYS*TICKS_PER_DAY)
times:raze {[j] {[i] 00:00:00.000+i*(floor 86400000%TICKS_PER_DAY)*00:00:00.001} each til TICKS_PER_DAY} each til DAYS
idx:(DAYS*TICKS_PER_DAY)?count SYMBOLS
symbols:SYMBOLS[idx]
sizes:(DAYS*TICKS_PER_DAY)?1000
prices:{[i] (first 1?100)+100+100*i} each idx
is_buy:(DAYS*TICKS_PER_DAY)?0b

// create a table with trades
trades:([] dates:dates+times; symbols:symbols; prices:prices; sizes:sizes; is_buy:is_buy)

// create a splayed partition per day
{[dd]; trades_splayed: select from trades where dates within (dd;(dd+1) - 0D00:00:00.001); sss:`$(":db/",(string dd),"/trades_splayed/"); sss set .Q.en[`:db;] trades_splayed} each (2025.01.01 + til DAYS)