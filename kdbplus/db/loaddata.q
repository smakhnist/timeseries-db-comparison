// constants
DAYS:100
TICKS_PER_DAY:10000
SYMBOLS:`AAPL`MSFT`GOOG

// create splayed table
{[dd] ;
dates:TICKS_PER_DAY#dd;
times:{[i] 00:00:00.000+i*(floor 86400000%TICKS_PER_DAY)*00:00:00.001} each til TICKS_PER_DAY;
idx:TICKS_PER_DAY?count SYMBOLS;
symbols:SYMBOLS[idx];
sizes:TICKS_PER_DAY?1000;
prices:{[i] (first 1?100)+100+100*i} each idx;
is_buy:(TICKS_PER_DAY)?0b;
trades:([] dates:dates+times; symbols:symbols; prices:prices; sizes:sizes; is_buy:is_buy);
sss:`$(":db/",(string dd),"/trades_splayed/");
sss set .Q.en[`:db;] trades;
 } each 2025.01.01+til DAYS