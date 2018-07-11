# Query information

Query Performance Insight (QPI) library enables you to find information about the queries that are executing in SQL Database Engine.
This library provides wrapper views that are using Query Store views, so Query Store must be enabled.

## Query information

QPI library enables you to easily get information about the queries that have been executing. `qpi.query_texts` view returns information about all know queries that are executed in past:

```
SELECT *
FROM qpi.query_texts;
```
`qpi.query_texts` view returns the following information:

| Column | Description |
| --- | --- |
| text | Text of T-SQL command. |
| params | Parameter values used in the T-SQL command. |
| query_text_id | Unique id of the text in T-SQL command. | 
| queries | Comma separated list of (query id, context settings id) pairs for all queries that match the query text. |

> One T-SQL query text might have several actual queries in **Query Store** terminology because the same query text might be executed under different conditions. This is the reason why we have a list of all possible query ids for one query text.

In order to get the information about the particular queries, you can use the following view:
```
SELECT *
FROM qpi.queries;
```
`qpi.queries` view returns the following information:

| Column | Description |
| --- | --- |
| query_id | Unique id of the query. |
| text | Text of T-SQL command. |
| params | Parameter values used in the T-SQL command. |
| query_text_id | Unique id of the text in T-SQL command. | 
| context_setting_id | Id of the context setting that describes the environment parameters that was configured while the query was executed. |

In order to get the information about the queries including context settings you can use the following query:
```
SELECT *
FROM qpi.queries_ex q
```

This query will return the same information as the previous one, but it will also include context settings options. Function `qpi.decode_options( set_options )` will decode context settings options.

## Current queries

**QPI** library enables you to get the list of currently running queries using `qpi.running_queries` view:
```
SELECT *
FROM qpi.running_queries;
```
