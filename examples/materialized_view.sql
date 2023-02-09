SELECT id,
  any(actor_name) as name,
  uniqExact(movie_id)    as num_movies,
  avg(rating)                as avg_rating,
  max(created_at) as updated_at
FROM (
  SELECT actors.id as id,
       concat(actors.first_name, ' ', actors.last_name) as actor_name,
       movies.id as movie_id,
       movies.rating as rating,
       created_at
  FROM  imdb.actors
        JOIN imdb.roles ON roles.actor_id = actors.id
        LEFT OUTER JOIN imdb.movies ON movies.id = roles.movie_id
)
GROUP BY id