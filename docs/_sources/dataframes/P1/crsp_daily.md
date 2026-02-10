# Dataframe: `P1:crsp_daily` - CRSP Daily Stock Data (Russell 1000 Proxy)

Daily CRSP stock data for the paper window (2000-01-01 to 2019-06-30). Includes returns (ret, retx), prices, shares, adjustment factors, and delisting returns.
A Russell 1000 proxy universe is constructed by ranking daily nominal market capitalization and keeping the top 1000 stocks per day.



## DataFrame Glimpse

```
Rows: 4903000
Columns: 22
$ date              <datetime[ns]> 2016-01-08 00:00:00
$ permno                     <i64> 10026
$ permco                     <i64> 7976
$ cusip                      <str> '46603210'
$ ret                        <f64> -0.002075
$ retx                       <f64> -0.002075
$ prc                        <f64> 110.59
$ openprc                    <f64> 111.02
$ vol                        <f64> 86617.0
$ shrout                     <f64> 18677000.0
$ cfacshr                    <f64> 1.0
$ cfacpr                     <f64> 1.0
$ dlret                      <f64> None
$ dlretx                     <f64> None
$ dlstcd                     <i64> None
$ adj_shrout                 <f64> 18677000.0
$ adj_prc                    <f64> 110.59
$ adj_openprc                <f64> 111.02
$ market_cap                 <f64> 2065489430.0
$ market_cap_adj             <f64> 2065489430.0
$ rank                       <f64> 989.0
$ __index_level_0__          <i64> 18615


```

## Dataframe Manifest

| Dataframe Name                 | CRSP Daily Stock Data (Russell 1000 Proxy)                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [crsp_daily](../dataframes/P1/crsp_daily.md)                                       |
| Data Sources                   | CRSP                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Pulled via WRDS using crsp.dsf, msenames, and msedelist tables; filtered to common stocks and Russell 1000 proxy.                                                    |
| Data available up to (min)     | N/A (large file)                                                             |
| Data available up to (max)     | N/A (large file)                                                             |
| Dataframe Path                 | /Users/reecevdw/UChicago/FINM 32900/old_project/p17_chen_kelly_xiu_2022/_data/CRSP_DAILY_PAPER_UNIVERSE.parquet                                                   |


**Linked Charts:**


- [P1:crsp_market_cap](../../charts/P1.crsp_market_cap.md)



## Pipeline Manifest

| Pipeline Name                   | p17_chen_kelly_xiu_2022                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [P1](../index.md)              |
| Lead Pipeline Developer         | Reece VanDeWeghe             |
| Contributors                    | Andrew Moukabary           |
| Git Repo URL                    | https://github.com/reecevdw/p17_chen_kelly_xiu_2022                        |
| Pipeline Web Page               | <a href="file:///Users/reecevdw/UChicago/FINM 32900/old_project/p17_chen_kelly_xiu_2022/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-09 21:03:13           |
| OS Compatibility                |  |
| Linked Dataframes               |  [P1:crsp_daily](../dataframes/P1/crsp_daily.md)<br>  [P1:ravenpack_djpr](../dataframes/P1/ravenpack_djpr.md)<br>  [P1:ravenpack_crsp_merged](../dataframes/P1/ravenpack_crsp_merged.md)<br>  |


