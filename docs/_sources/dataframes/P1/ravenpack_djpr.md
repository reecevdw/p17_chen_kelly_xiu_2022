# Dataframe: `P1:ravenpack_djpr` - RavenPack Dow Jones & PR Equities News

RavenPack Dow Jones & PR Edition equities news (US firms only) for 2000-01-01 to 2019-06-30.
Filtered to relevance >= 90 and restricted to single-firm stories (one distinct rp_entity_id per provider story).
Contains event metadata, similarity measures, headline, and composite sentiment score (css).



## DataFrame Glimpse

```
Rows: 28809067
Columns: 26
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


```

## Dataframe Manifest

| Dataframe Name                 | RavenPack Dow Jones & PR Equities News                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [ravenpack_djpr](../dataframes/P1/ravenpack_djpr.md)                                       |
| Data Sources                   | RavenPack                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Pulled via WRDS from ravenpack_dj.rpa_djpr_equities_YYYY tables; filtered to US firms, relevance >= 90, single-firm stories.                                                    |
| Data available up to (min)     | N/A (large file)                                                             |
| Data available up to (max)     | N/A (large file)                                                             |
| Dataframe Path                 | /Users/reecevdw/UChicago/FINM 32900/old_project/p17_chen_kelly_xiu_2022/_data/ravenpack_djpr.parquet                                                   |


**Linked Charts:**


- [P1:ravenpack_article_volume](../../charts/P1.ravenpack_article_volume.md)



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


