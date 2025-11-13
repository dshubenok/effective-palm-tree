-- Считает почасовые просмотры (приросты cumulative) по поисковым запросам
WITH phrases AS (
    SELECT
        phrase,
        campaign_id,
        toStartOfHour(dt) AS hour_ts,
        max(views) AS cumulative_views
    FROM phrases_views
    WHERE dt >= date_trunc('day', now()) -- сегодня
      AND campaign_id = {campaign_id:Int32}
    GROUP BY phrase, campaign_id, hour_ts
),
diffs AS (
    SELECT
        phrase,
        campaign_id,
        hour_ts,
        cumulative_views - lag(cumulative_views, 1, 0)
            OVER (PARTITION BY phrase, campaign_id ORDER BY hour_ts) AS hour_views
    FROM phrases
)
SELECT
    phrase,
    arraySort(
        x -> -x.1,
        arrayFilter(
            x -> x.2 > 0,
            groupArray((toHour(hour_ts), hour_views))
        )
    ) AS views_by_hour
FROM diffs
GROUP BY phrase
ORDER BY phrase;
