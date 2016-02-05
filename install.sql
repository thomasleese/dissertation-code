CREATE TABLE users (
    id INT(11) PRIMARY KEY NOT NULL,
    login VARCHAR(100),
    avatar_url VARCHAR(200),
    gravatar_id VARCHAR(100),
    name VARCHAR(100),
    company VARCHAR(100),
    blog VARCHAR(200),
    location VARCHAR(100),
    location_country VARCHAR(3),
    location_latitude DECIMAL(10,8),
    location_longitude DECIMAL(11,8),
    email VARCHAR(200),
    hireable TINYINT(1),
    bio TEXT,
    gender VARCHAR(1),
    gender_probability DECIMAL(5,4)
);