r:select open:first prices, high:max prices, low:min prices, close:last prices, volume:sum sizes
      by bucket
    from update bucket:0D00:05 xbar dates
           from trades
          where (symbols=`AAPL) & dates within (2025.02.01;2025.02.04)
r