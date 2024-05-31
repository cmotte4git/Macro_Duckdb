--- Create a parquet file from an external source
CREATE OR REPLACE TABLE spotify_tracks AS (
FROM
'https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet?download=true'
);

--- Data check
SHOW TABLES;
SELECT * FROM main.spotify_tracks LIMIT 100;
SELECT
	COUNT()
FROM
	spotify_tracks;

---- Create reusable aggration macro
CREATE OR REPLACE MACRO custom_summarize() AS TABLE (
    WITH metrics AS (
        FROM any_cte 
        SELECT 
            {
                name: first(alias(COLUMNS(*))),
                type: first(typeof(COLUMNS(*))),
                max: max(COLUMNS(*))::VARCHAR,
                min: min(COLUMNS(*))::VARCHAR,
                approx_unique: approx_count_distinct(COLUMNS(*)),
                nulls: count(*) - count(COLUMNS(*)),
            }
    ), stacked_metrics AS (
        UNPIVOT metrics 
        ON COLUMNS(*)
    )
    SELECT value.* FROM stacked_metrics
);

--- Execute custom_summarize with spotify data
WITH any_cte AS (FROM spotify_tracks)
FROM custom_summarize();



--- Create magro to aggregate columns from a given tables
CREATE OR REPLACE MACRO dynamic_aggregates(
        included_columns,
        excluded_columns,
        aggregated_columns,
        aggregate_function
    ) AS TABLE ( WITH metrics AS (
        FROM any_cte 
    SELECT 
        -- Use a COLUMNS expression to only select the columns
        -- we include or do not exclude
        COLUMNS(c -> (
            -- If we are not using an input parameter (list is empty),
            -- ignore it
            (list_contains(included_columns, c) OR
             len(included_columns) = 0)
            AND
            (NOT list_contains(excluded_columns, c) OR
             len(excluded_columns) = 0)
            )),
        -- Use the list_aggregate function to apply an aggregate
        -- function of our choice
        list_aggregate(
            -- Convert to a list (to enable the use of list_aggregate)
            list(
                -- Use a COLUMNS expression to choose which columns
                -- to aggregate
                COLUMNS(c -> list_contains(aggregated_columns, c))
            ), aggregate_function
        )
    GROUP BY ALL -- Group by all selected but non-aggregated columns
    ORDER BY ALL -- Order by each column from left to right 
    )
    SELECT * FROM metrics
);

--- Check the two user defined macros with internal is FALSE filter 
SELECT * FROM
 duckdb_functions() where internal is FALSE ;

 
--- Execute dynamic_aggregates with spotify table
FROM main.spotify_tracks ;
WITH any_cte AS (FROM spotify_tracks)
FROM dynamic_aggregates(
    ['track_genre'], [], ['duration_ms','popularity'], 'avg');
--- first list is group by column, second list to exclude columns, third to include columns, fourth to select aggregation
   
 --- last exemple, we have a dynamic table and a dynamic table on a column
   ---- Usefull for an ETL_Timestamp
CREATE OR REPLACE MACRO variable_code(genre) AS TABLE
WITH results AS (FROM any_cte) SELECT * FROM results  WHERE track_genre = genre;

WITH any_cte AS (FROM spotify_tracks)
FROM variable_code('jazz'); 
    
   
   