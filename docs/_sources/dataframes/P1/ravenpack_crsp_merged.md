# Dataframe: `P1:ravenpack_crsp_merged` - RavenPack × CRSP Merged Dataset

Merged dataset created by:
1) Building a crosswalk from CRSP permno to RavenPack rp_entity_id using CRSP historical NCUSIP and RavenPack ISIN (CUSIP8 = SUBSTRING(isin,3,8)).
2) Attaching permno to RavenPack via rp_entity_id (left join).
3) Merging to CRSP daily on (permno, date) using the RavenPack timestamp_utc normalized to date.



## DataFrame Glimpse

```
Rows: 28809067
Columns: 47
$ timestamp_utc         <datetime[ns]> 2000-01-01 17:52:28
$ rp_story_id                    <str> '52D513EA9040443376A6B1747B0FA8CC'
$ rp_entity_id                   <str> '047E26'
$ entity_type                    <str> 'COMP'
$ entity_name                    <str> 'Sprint Corp.'
$ country_code                   <str> 'US'
$ relevance                      <f64> 92.0
$ event_sentiment_score          <f64> None
$ event_relevance                <f64> None
$ event_similarity_key           <str> None
$ event_similarity_days          <f64> None
$ topic                          <str> None
$ rp_group                       <str> None
$ rp_type                        <str> None
$ sub_type                       <str> None
$ property                       <str> None
$ fact_level                     <str> None
$ category                       <str> None
$ news_type                      <str> 'FULL-ARTICLE'
$ rp_source_id                   <str> 'B5569E'
$ source_name                    <str> 'Dow Jones Newswires'
$ provider_id                    <str> 'DJ'
$ provider_story_id              <str> 'DN20000101000112'
$ headline                       <str> 'Sprint Says No Y2K Rollover Problems Reported >FON'
$ css                            <f64> -0.06
$ year                           <i64> 2000
$ permno                         <i64> 14040
$ date                  <datetime[ns]> 2000-01-01 00:00:00
$ permco                         <i64> None
$ cusip                          <str> None
$ ret                            <f64> None
$ retx                           <f64> None
$ prc                            <f64> None
$ openprc                        <f64> None
$ vol                            <f64> None
$ shrout                         <f64> None
$ cfacshr                        <f64> None
$ cfacpr                         <f64> None
$ dlret                          <f64> None
$ dlretx                         <f64> None
$ dlstcd                         <i64> None
$ adj_shrout                     <f64> None
$ adj_prc                        <f64> None
$ adj_openprc                    <f64> None
$ market_cap                     <f64> None
$ market_cap_adj                 <f64> None
$ rank                           <f64> None


```

## Dataframe Manifest

| Dataframe Name                 | RavenPack × CRSP Merged Dataset                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [ravenpack_crsp_merged](../dataframes/P1/ravenpack_crsp_merged.md)                                       |
| Data Sources                   | RavenPack, CRSP                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Linked RavenPack to CRSP using WRDS-recommended CUSIP/NCUSIP method; merged on permno and date.                                                    |
| Data available up to (min)     | N/A (large file)                                                             |
| Data available up to (max)     | N/A (large file)                                                             |
| Dataframe Path                 | /Users/reecevdw/UChicago/FINM 32900/old_project/p17_chen_kelly_xiu_2022/_data/ravenpack_crsp_merged.parquet                                                   |


**Linked Charts:**


- [P1:ravenpack_sentiment_distribution](../../charts/P1.ravenpack_sentiment_distribution.md)



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


