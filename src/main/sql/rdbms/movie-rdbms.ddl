---------------------------------------------
--Movie information: MovieID::Title::Genres
---------------------------------------------
DROP TABLE if EXISTS ott.movies;
CREATE TABLE ott.movies (
    movie_id INT NOT NULL,
    title STRING NULL,
    genres STRING NULL,
    PRIMARY KEY (movie_id)
);