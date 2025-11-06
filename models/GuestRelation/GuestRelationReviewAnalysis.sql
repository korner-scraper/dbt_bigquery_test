{{ config(materialized='table', alias='GR_TEST') }}

WITH src AS (
  SELECT DISTINCT
    DATE(Avis_created_date)                           AS date_publication,
    tag,
    reviewer                                          AS client,
    Avis_client,
    ai_translation,
    ai_suggestion,
    rating                                            AS note,
    source,
    sentiment_score,
    insertion_date,
    REGEXP_REPLACE(ID, r' \d{2}:\d{2}:\d{2}', '')     AS id_norm
  FROM {{ source('bigquery_source', 'GuestRelation_ReviewAnalysis') }}
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id_norm
      ORDER BY insertion_date DESC
    ) AS rn
  FROM src
)
SELECT
  date_publication,
  id_norm AS id,
  tag,
  client,
  Avis_client,
  ai_translation,
  ai_suggestion,
  note,
  source,
  sentiment_score,
  insertion_date
FROM dedup
WHERE rn = 1
