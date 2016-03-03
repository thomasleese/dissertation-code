CREATE TABLE users (
    id INT(11) PRIMARY KEY NOT NULL,
    login VARCHAR(100),
    name VARCHAR(100),
    company VARCHAR(200),
    blog VARCHAR(200),
    location VARCHAR(100),
    location_country VARCHAR(3),
    location_latitude DECIMAL(10,8),
    location_longitude DECIMAL(11,8),
    hireable TINYINT(1),
    bio TEXT,
    gender VARCHAR(1),
    gender_probability DECIMAL(5,4),
    deleted BOOLEAN
);

CREATE TABLE repositories (
    owner VARCHAR(100),
    name VARCHAR(100),

    language VARCHAR(100),
    stargazers INT,
    has_downloads BOOLEAN,
    is_fork BOOLEAN,
    has_issues BOOLEAN,
    watchers INT,
    open_issues INT,
    size INT,
    has_wiki INT,
    forks INT,

    PRIMARY KEY (owner, name)
)
