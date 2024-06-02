DROP TABLE distributors;

CREATE TABLE distributors (
    did     integer,
    name    varchar(40),
    description text,
    alpha_level float8,
    created TIMESTAMP,
    hokage boolean
);

INSERT INTO distributors (did, name, description, alpha_level, created, hokage) VALUES (1, 'Naruto', 'description text', '0.9', now(), true);
INSERT INTO distributors (did, name, alpha_level, created, hokage) VALUES (2, 'Sasuke', '0.9', now(), false);
INSERT INTO distributors (name, description, alpha_level, created, hokage) VALUES ('Itachi', 'description text', '1.01', now(), false);
INSERT INTO distributors (did, name, description, alpha_level, created, hokage) VALUES (3, 'Jiraiya', 'description text', now(), false);
INSERT INTO distributors (did, name, description, alpha_level, hokage) VALUES (4, 'Tsunade', 'description text', '0.89', true);
INSERT INTO distributors (did, name, description, alpha_level, created) VALUES (5, 'Pain', 'description text', '1.0', now());
INSERT INTO distributors (did, description, created) VALUES (6, 'description text', now());


SELECT * FROM distributors;