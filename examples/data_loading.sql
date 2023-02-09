CREATE DATABASE imdb;

CREATE TABLE imdb.actors
(
    id         UInt32,
    first_name String,
    last_name  String,
    gender     FixedString(1)
) ENGINE = CnchMergeTree ORDER BY (id, first_name, last_name, gender);

INSERT INTO imdb.actors VALUES (1, 'Scarlett', 'Johansson', 'F');
INSERT INTO imdb.actors VALUES (2, 'Robert', 'Downey', 'M');
INSERT INTO imdb.actors VALUES (3, 'Chris', 'Hemsworth', 'M');
INSERT INTO imdb.actors VALUES (4, 'Chris', 'Pratt', 'M');

CREATE TABLE imdb.movies
(
    id   UInt32,
    name String,
    year UInt32,
    rating Float32
) ENGINE = CnchMergeTree ORDER BY (id, name, year);

INSERT INTO imdb.movies VALUES (1, 'Lost in Translation', 2003, 7.7);
INSERT INTO imdb.movies VALUES (2, 'Iron Man', 2008, 7.9);
INSERT INTO imdb.movies VALUES (3, 'Sherlock Holmes', 2009, 7.6);
INSERT INTO imdb.movies VALUES (4, 'Avengers Endgame', 2019, 8.4);
INSERT INTO imdb.movies VALUES (5, '12 Strong', 2018, 6.5);
INSERT INTO imdb.movies VALUES (6, 'Thor', 2017, 7.9);
INSERT INTO imdb.movies VALUES (7, 'Guardians of the Galaxy', 2014, 8.0);
INSERT INTO imdb.movies VALUES (8, 'The Lego Movie', 2014, 7.7);

CREATE TABLE imdb.roles
(
    created_at DateTime DEFAULT now(),
    actor_id   UInt32,
    movie_id   UInt32,
    role_name  String
) ENGINE = CnchMergeTree ORDER BY (actor_id, movie_id);

INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(1, 1, 'Charlotte');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(2, 2, 'Tony Stark');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(2, 3, 'Sherlock Holmes');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(2, 4, 'Tony Stark');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(3, 5, 'Captain Mitch');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(3, 6, 'Thor');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(4, 7, 'Star Lord');
INSERT INTO imdb.roles (actor_id, movie_id, role_name) VALUES(4, 8, 'Emmet');