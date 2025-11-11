-- Возвращает просмотры по поисковым фразам по часам за сегодня
SELECT
    phrase,
    arraySort(
        x -> -x.1,
        groupArray((hour, views_count))
    ) AS views_by_hour
FROM
(
    SELECT
        phrase,
        toHour(event_time) AS hour,
        sum(views) AS views_count
    FROM search_phrase_views
    WHERE campaign_id = {campaign_id:UInt64}
      AND toDate(event_time) = today()
    GROUP BY phrase, hour
)
GROUP BY phrase
ORDER BY phrase;
