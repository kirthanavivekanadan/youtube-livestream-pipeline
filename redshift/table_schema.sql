DROP TABLE IF EXISTS youtube_live_streams;
CREATE TABLE youtube_live_streams (
    video_id VARCHAR(255),
    title VARCHAR(500),
    description VARCHAR(1000),
    channel_title VARCHAR(255),
    channel_id VARCHAR(255),
    published_at VARCHAR(50),
    thumbnail_url VARCHAR(1000),
    stream_url VARCHAR(1000),
    stream_duration_minutes FLOAT8,
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    fetched_at VARCHAR(50)
);
