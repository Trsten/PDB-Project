CREATE KEYSPACE IF NOT EXISTS social_network WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
DESCRIBE KEYSPACE social_network;
USE social_network;

CREATE TABLE IF NOT EXISTS user (
    user_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT PRIMARY KEY,
    address TEXT,
	password TEXT,
	phone TEXT,
	followers set<TEXT>
) WITH comment = 'Records of users registered in social network.';

CREATE TABLE IF NOT EXISTS post (
    user_email TEXT,
    time TIMEUUID,
    title TEXT,
    content TEXT,
	rating DOUBLE,
	comment_count INT,
	rating_count INT,
	PRIMARY KEY ( (user_email), time)
) WITH comment = 'Post of selected user.';

CREATE TABLE IF NOT EXISTS comment (
    post_id TIMEUUID,
    time TIMESTAMP,
    user_email TEXT,
    content TEXT,
PRIMARY KEY ((post_id), time, user_email)
) WITH comment = 'Comment of selected post.';

CREATE TABLE IF NOT EXISTS rating (
    post_id TIMEUUID,
    value DOUBLE,
    user_email TEXT,
PRIMARY KEY ((post_id), user_email)
) WITH comment = 'Rating of selected post.';

CREATE TABLE IF NOT EXISTS top_rating (
    user_email TEXT,
    rating DOUBLE,
    post_id UUID,
    PRIMARY KEY (post_id, rating)
) WITH CLUSTERING ORDER BY (rating DESC);
