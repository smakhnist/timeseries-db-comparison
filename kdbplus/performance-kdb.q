// load data
\l db

r: select
open: first prices,
      high: max prices,
      low:  min prices,
      close: last prices,
      volume: sum sizes
by bucket: 0D00:05 xbar dates
    from trades_splayed
   where date within (2025.02.01;2025.02.04), symbols = `AAPL
r