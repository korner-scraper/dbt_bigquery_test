{{ 
    config(
        materialized='table', 
        alias="mart_guest_relation_retour_xano",
        tags=["GR"]
        ) 
}}



select id, date_de_publication,bq_id,polarity,sub_category,tag
from
(select distinct a.id, date_de_publication,bq_id,polarity,sub_category,tag
from
(SELECT  id, bq_id, client, date_de_publication,hotel_response,concat(left(bq_id,3),"-",date(date_de_publication),"-",client,"-",rating) as BQV2
FROM `korner-datalake.Xano.GuestRelation` 
WHERE guest_relation_traite is TRUE
and date(insertion_date) = current_date ) a
left join
(select * from
(select distinct CONCAT(tag, '-', date(published_date), '-', reviewer_name, '-', rating) as ID,polarity,sub_category,tag
from(
SELECT a.*,polarity,strength,sub_category
FROM (
SELECT DISTINCT
                    review_id,
                    tag,
                    client_review,
                    published_date,
                    reviewer_name,
                    rating AS Rating,
                    round(rating/20,1) as rating_of_5,
                    cast((round(cast(sentiment_score as numeric), 2)) as string) AS Sentiment_score,
                    source,
                    hotel_response,
                    respond_url,
                    insertion_date,
                    first_insertion,
                    hotel_id
                FROM `korner-datalake.ReviewInsight.Avis_client_en_ligne_KPI`
                WHERE Groupe = "Groupe Korner"
                  AND can_respond = 1 
            ) AS a
            LEFT JOIN 
(select distinct id,review_review_id,polarity,max(strength) over(partition by review_review_id,hotel_id,sub_category,polarity order by sub_category) as strength,sub_category,hotel_id 
from 
(select id,review_review_id,polarity,strength, label,
CASE WHEN sub_category IN ("Hygiene", "Sanitary Safety", "Pandemic Precautions", "Covid") THEN "Propreté"
                        WHEN sub_category IN ("Personnel", "Reception", "Checkin", "Checkout", "Support") THEN "Accueil"
                        WHEN sub_category IN ("Breakfast", "Drink", "Food") THEN "PDJ"
                        WHEN sub_category IN ("Sustainable Travel", "Surrounding Area", "Decor") THEN "Travaux"
                        WHEN sub_category IN ("Room", "Bathroom") THEN "Taille"
                        WHEN sub_category IN ("Sound", "Entertainment Facilities") THEN "Bruyant"
                        WHEN sub_category IN ("Airconditioning", "Internet", "Transport", "Gym", "Sports Facilities", "Spa","Light") THEN "Équipement"
                        WHEN sub_category IN ("Bathroom") THEN "Salle de bain"
                        WHEN sub_category IN ("Odor", "Humidity") THEN "Odeur"
                        WHEN sub_category IN ("Staff", "Housekeeping", "Bar", "Restaurant", "Shopping", "Delivery","Value for money") THEN "Employé"
                    END AS sub_category,hotel_id
                    from
                    (
                SELECT
                    id,
                    review_review_id,
                    polarity,
                    strength,
                    rating.label,
                    topic.label AS sub_category,
                    hotel_id
                FROM `korner-datalake.Olery.Opinions_olery` op,
                     UNNEST(ratings) AS rating,
                     UNNEST(rating.topics) AS topic
                WHERE op.insertion_date = (
                    SELECT MAX(insertion_date)
                    FROM `korner-datalake.Olery.Opinions_olery` op1
                    WHERE op.id = op1.id
                      AND op.review_review_id = op1.review_review_id
                )
            ))) AS b
            ON a.review_id = b.review_review_id
               AND a.hotel_id = b.hotel_id
        ))
group by 1, 2, 3, 4 ) b
on a.BQV2=b.ID)
where polarity is not null

