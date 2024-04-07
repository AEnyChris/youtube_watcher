CREATE TABLE youtube_changes WITH (KAFKA_TOPIC='youtube_changes') AS
SELECT
   video_id,
   latest_by_offset(title) AS title,
   latest_by_offset(comments, 2)[1] AS comment_previous,
   latest_by_offset(comments, 2)[2] AS comment_current,
   latest_by_offset(views, 2)[1] AS views_previous,
   latest_by_offset(views, 2)[2] AS views_current,
   latest_by_offset(likes, 2)[1] AS likes_previous,
   latest_by_offset(likes, 2)[2] AS likes_current
FROM  YOUTUBE_VIDEOS
GROUP BY video_id;


SELECT *
FROM  YOUTUBE_CHANGES
WHERE likes_previous <> likes_current
EMIT CHANGES;

INSERT INTO telegram_outbox (
  `chat_id`,
  `text`
) VALUES (
  '1208978144',
  'Jesus loves me'
);


CREATE STREAM youtube_changes_stream
WITH (
  KAFKA_TOPIC='youtube_changes',
  VALUE_FORMAT='avro'
);

INSERT INTO telegram_outbox
SELECT
    '1208978144' AS `chat_id`,
    CONCAT (
      'Likes changed:',
      CAST(likes_previous AS STRING),
      '=>',
      CAST(likes_current AS STRING),
      '.',
      title
    ) AS text
FROM  YOUTUBE_CHANGES _STREAM
WHERE likes_current <> likes_previous;


