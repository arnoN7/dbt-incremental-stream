{% docs conso_client %}
## Client 360
### Description
**Table consolidating clients from all company sales channels** : RETAIL, WEB and PARTNER WEB Marketplace. Contains **the last update of clients only**.


### Sources and freshness
**source list**

| SOURCE NAME | DESCRIPTION  | INGESTION MODE | FRESHNESS  |
|-------------|--------------|----------------|------------|
| RETAIL APP  | Clients flow ingested in delta from the CDC plugged on the retail app      |  ![kafka](assets/kafka_logo70.png)  | < 10 min |
| WEB SHOP     | Clients flow ingested natively pushed to Kafka by the web shop |  ![kafka](assets/kafka_logo70.png)  | < 10 min |
| WEB SHOP PARTNER  | Client flow pushed by our Marketplace partner | ![file](assets/file30.png) File | 1 day |

**Note**
Use on `incremental_stream` materialisation collecting data from [Streams](https://docs.snowflake.com/en/user-guide/streams-intro) 

![stream](assets/stream.gif)



### Usage example
**List all clients**
```
SELECT ID, SOURCE, FIRST_NAME, LAST_NAME, BIRTHDATE FROM conso_client_multiple_streams
```

**View distribution of client per birthdate**
```
SELECT YEAR(BIRTHDATE) AS YEAR, COUNT(*) as NB FROM conso_client_multiple_streams GROUP BY YEAR(BIRTHDATE) ORDER BY YEAR DESC 
```
{% enddocs %}
