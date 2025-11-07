<<<<<<< HEAD

--create table `korner-datalake.ReviewInsight.GuestRelation_ReviewAnalysis_maxInsertion` as

{{ 
    config(
        alias="mart_guest_relation_reviews_latest",
        tags=["GR"]
        ) 
}}

With base as (

(select distinct a.*, b.statut, b.username as Hote, b.type_user as Role, row_number() over(
partition by a.id order by case b.statut when "Répondu" then 1
when "En attente de validation" then 2
when "À répondre" then 3 else 4 end ) as rn
from
(SELECT distinct date(Avis_created_date) as date_publication,--id, 
tag, reviewer as client, Avis_client, ai_translation, ai_suggestion, rating as note, source, sentiment_score, insertion_date, REGEXP_REPLACE(ID, r' \d{2}:\d{2}:\d{2}', '') as ID
FROM `korner-datalake.ReviewInsight.GuestRelation_ReviewAnalysis` rn
where rn.insertion_date = (Select max(insertion_date)
                            from `korner-datalake.ReviewInsight.GuestRelation_ReviewAnalysis`rn1
                            where (REGEXP_REPLACE(rn.ID, r' \d{2}:\d{2}:\d{2}', '')) = REGEXP_REPLACE(rn1.ID, r' \d{2}:\d{2}:\d{2}', ''))
)a

left join

`korner-datalake.Xano.GuestRelation`b
on a.id = (REGEXP_REPLACE(b.bq_id, r' \d{2}:\d{2}:\d{2}', ''))
))
select * except(insertion_date, rn) 
from base where rn = 1 
order by date_publication desc
