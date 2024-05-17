DROP TABLE distributors;

CREATE TABLE distributors (
    did     integer,
    name    varchar(40)
);

INSERT INTO distributors (did, name) VALUES (1, 'Naruto');
INSERT INTO distributors (did, name) VALUES (2, 'Sasuke');
INSERT INTO distributors (did, name) VALUES (3, 'Itachi');
INSERT INTO distributors (did, name) VALUES (4, 'Jiraiya');
INSERT INTO distributors (did, name) VALUES (5, 'Tsunade');
INSERT INTO distributors (did, name) VALUES (6, 'Pain');

SELECT * FROM distributors;

create foreign table distributors_parquet (
    did           int,
    name   varchar(40)
)
server parquet_srv
options (
    filename 'sample.parquet'
);