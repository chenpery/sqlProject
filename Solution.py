from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor

# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "BEGIN;" +
            "DROP TABLE IF EXISTS CriticTable CASCADE; CREATE TABLE CriticTable(critic_id INTEGER NOT NULL, name TEXT NOT NULL," +
            "PRIMARY KEY(critic_id), CHECK(critic_id>0));" +
            "DROP TABLE IF EXISTS MovieTable CASCADE; CREATE TABLE MovieTable(movie_name TEXT NOT NULL, year INTEGER NOT NULL, genre TEXT NOT NULL," +
            "PRIMARY KEY(movie_name, year), CHECK(year>= 1895), CHECK(genre = \'Drama\' OR genre=\'Action\' OR genre=\'Comedy\' OR genre=\'Horror\'));" +
            "DROP TABLE IF EXISTS ActorTable CASCADE; CREATE TABLE ActorTable(actor_id INTEGER NOT NULL, name TEXT NOT NULL, age INTEGER NOT NULL, height INTEGER NOT NULL," +
            "PRIMARY KEY(actor_id), CHECK(actor_id > 0 AND age > 0 AND height > 0));" +
            "DROP TABLE IF EXISTS StudioTable CASCADE; CREATE TABLE StudioTable(studio_id INTEGER NOT NULL, name TEXT NOT NULL," +
            "PRIMARY KEY(studio_id), CHECK(studio_id > 0));" +
            "DROP TABLE IF EXISTS CriticMovieRela CASCADE; CREATE TABLE CriticMovieRela(critic_id, movie_name, year, rating INTEGER," +
            "FOREIGN KEY(critic_id) REFERENCES CriticTable(critic_id) ON DELETE CASCADE," +
            "FOREIGN KEY(movie_name, year) REFERENCES MovieTable(movie_name, year) ON DELETE CASCADE," +
            "UNIQUE (critic_id, movie_name, year), CHECK(rating >=1 and rating <= 5));" +
            "DROP TABLE IF EXISTS ActorInMovieRela CASCADE; CREATE TABLE ActorInMovieRela(movie_name, year, actor_id, salary INTEGER," +
            "FOREIGN KEY(actor_id) REFERENCES ActorTable(actor_id) ON DELETE CASCADE," +
            "FOREIGN KEY(movie_name,year) REFERENCES MovieTable(movie_name, year) ON DELETE CASCADE," +
            "PRIMARY KEY (movie_name,year,actor_id), CHECK(salary > 0));" +
            "DROP TABLE IF EXISTS ActorRoleInMovieRela CASCADE; CREATE TABLE ActorRoleInMovieRela(movie_name, year, actor_id, roles TEXT NOT NULL," +
            "FOREIGN KEY(movie_name, year, actor_id) REFERENCES ActorInMovieRela(movie_name, year, actor_id) ON DELETE CASCADE," +
            "CHECK(all roles IS NOT NULL));" +
            "DROP TABLE IF EXISTS MovieInStudioRela CASCADE; CREATE TABLE MovieInStudioRela(studio_it, movie_name, year, budget INTEGER, revenue INTEGER default(0)," +
            "FOREIGN KEY(studio_it) REFERENCES StudioTable(studio_it) ON DELETE CASCADE," +
            "FOREIGN KEY(movie_name,year) REFERENCES MovieTable(movie_name, year) ON DELETE CASCADE," +
            "UNIQUE (studio_it, movie_name,year), CHECK(budget >= 0), CHECK( revenue >= 0));" +
            "DROP VIEW IF EXISTS NumRolesInMovieXActor CASCADE; CREATE VIEW NumRolesInMovieXActor AS SELECT actor_id, movie_name, year, COUNT(roles) AS roles_num FROM ActorRoleInMovieRela GROUP BY actor_id, movie_name, year;" +
            "DROP VIEW IF EXISTS MovieAvgRate CASCADE; CREATE VIEW MovieAvgRate AS SELECT movie_name, year, AVG(rating) FROM CriticMovieRela GROUP BY movie_name, year;" +
            "DROP VIEW IF EXISTS MovieRevenues CASCADE; CREATE VIEW MovieRevenues AS SELECT movie_name, year, SUM(revenue) AS revenue FROM MovieInStudioRela GROUP BY movie_name, year;" +
            "DROP VIEW IF EXISTS StudioRevenues CASCADE; CREATE VIEW StudioRevenues AS SELECT studio_id, year, SUM(revenue) AS revenue FROM MovieInStudioRela GROUP BY studio_id, year;" +
            "DROP VIEW IF EXISTS CtiticToStudio CASCADE; CREATE VIEW CtiticToStudio AS SELECT critic_id, studio_id, COUNT(movie_name) AS movies_num FROM CriticMovieRela INNER JOIN MovieInStudioRela GROUP BY critic_id, studio_id;" +
            "DROP VIEW IF EXISTS ActorToStudio CASCADE; CREATE VIEW ActorToStudio AS SELECT actor_id, studio_id, COUNT(movie_name) AS movies_num FROM ActorInMovieRela INNER JOIN MovieInStudioRela GROUP BY actor_id, studio_id;" +
            "DROP VIEW IF EXISTS ActorsInGenre CASCADE; CREATE VIEW ActorsInGenre AS SELECT DISTINCT actor_id, genre FROM ActorInMovieRela  LEFT OUTER JOIN MovieTable;" +
            "COMMIT;"
        )
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except DatabaseException.database_ini_ERROR as e:
        print(e)
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected = conn.execute(
                    "BEGIN;" +
                    "DELETE FROM CriticTable;" +
                    "DELETE FROM MovieTable;" +
                    "DELETE FROM ActorTable;" +
                    "DELETE FROM StudioTable;" +
                    "DELETE FROM CriticMovieRela;" +
                    "DELETE FROM ActorInMovieRela;" +
                    "DELETE FROM ActorRoleInMovieRela;" +
                    "DELETE FROM MovieInStudioRela; COMMIT;")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;" +
                     "DROP TABLE IF EXISTS CriticTable CASCADE;" +
                     "DROP TABLE IF EXISTS MovieTable CASCADE;" +
                     "DROP TABLE IF EXISTS ActorTable CASCADE;" +
                     "DROP TABLE IF EXISTS StudioTable CASCADE;" +
                     "DROP TABLE IF EXISTS CriticMovieRela CASCADE;" +
                     "DROP TABLE IF EXISTS ActorInMovieRela CASCADE;" +
                     "DROP TABLE IF EXISTS ActorRoleInMovieRela CASCADE;" +
                     "DROP TABLE IF EXISTS MovieInStudioRela CASCADE;" +
                     "DROP VIEW IF EXISTS NumRolesInMovieXActor CASCADE;" +
                     "DROP VIEW IF EXISTS MovieAvgRate CASCADE;" +
                     "DROP VIEW IF EXISTS MovieRevenues CASCADE;" +
                     "DROP VIEW IF EXISTS StudioRevenues CASCADE;" +
                     "DROP VIEW IF EXISTS CtiticToStudio CASCADE;" +
                     "DROP VIEW IF EXISTS ActorToStudio CASCADE;" +
                     "DROP VIEW IF EXISTS ActorsInGenre CASCADE; COMMIT;")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        assert isinstance(critic, Critic)  # TODO: is this allowed?
        query = sql.SQL("INSERT INTO CriticTable VALUES({critic_id}, {name})").format(
            critic_id=sql.Literal(critic.getCriticID()),
            name=sql.Literal(critic.getName()))

        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:  # Note - FOREIGN_KEY_VIOLATION not relevant here as it doesn't have one
        return ReturnValue.ERROR
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM CriticTable WHERE critic_id={id}").format(id=sql.Literal(critic_id))
        rows_effected, _ = conn.execute(query)

        conn.commit()  # TODO: Do we need it here or only rollback below?

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    critic = Critic()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From CriticTable WHERE critic_id={id}").format(id=sql.Literal(critic_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return critic.badCritic()
    except Exception:
        return critic.badCritic()

    finally:
        conn.close()
        if res.isEmpty():
            return critic.badCritic()
        else:
            critic.setCriticID(res.cols['critic_id'])
            critic.setName(res.cols['name'])
    return critic


def addActor(actor: Actor) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        assert isinstance(actor, Actor)
        query = sql.SQL("INSERT INTO ActorTable VALUES({actor_id}, {name}, {age}, {height})").format(
            actor_id=sql.Literal(actor.getActorID()),
            name=sql.Literal(actor.getActorName()),
            age=sql.Literal(actor.getAge()),
            height=sql.Literal(actor.getHeight()))
        conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:  # Note - FOREIGN_KEY_VIOLATION not relevant here as it doesn't have one
        return ReturnValue.ERROR
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM ActorTable WHERE actor_id={id}").format(id=sql.Literal(actor_id))
        rows_effected, _ = conn.execute(query)
        conn.commit()  # TODO: Do we need it here or only rollback below?

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS

    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    actor = Actor()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From ActorTable WHERE actor_id={id}").format(id=sql.Literal(actor_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return actor.badActor()
    except Exception:
        return actor.badActor()

    finally:
        conn.close()
        if res.isEmpty():
            return actor.badActor()
        else:
            actor.setActorID(res.cols['actor_id'])
            actor.setActorName(res.cols['name'])
            actor.setAge(res.cols['age'])
            actor.setHeight(res.cols['height'])
    return actor


def addMovie(movie: Movie) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        assert isinstance(movie, Movie)
        query = sql.SQL("INSERT INTO MovieTable VALUES({name}, {year}, {genre})").format(
            name=sql.Literal(movie.getMovieName()),
            year=sql.Literal(movie.getYear()),
            genre=sql.Literal(movie.getGenre()))
        conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:  # Note - FOREIGN_KEY_VIOLATION not relevant here as it doesn't have one
        return ReturnValue.ERROR
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM MovieTable WHERE movie_name={name} and year={year}").format(
            name=sql.Literal(movie_name),
            year=sql.Literal(year))
        rows_effected, _ = conn.execute(query)
        conn.commit()  # TODO: Do we need it here or only rollback below?

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS

    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    movie = Movie()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From MovieTable WHERE name={name} and year={year}").format(
            name=sql.Literal(movie_name),
            year=sql.Literal(year))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return movie.badMovie()
    except Exception:
        return movie.badMovie()

    finally:
        conn.close()
        if res.isEmpty():
            return movie.badMovie()
        else:
            movie.setMovieName(res.cols['movie_name'])
            movie.setYear(res.cols['year'])
            movie.setGenre(res.cols['genre'])
    return movie


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        assert isinstance(studio, Studio)
        query = sql.SQL("INSERT INTO StudioTable VALUES({studio_id}, {name})").format(
            studio_id=sql.Literal(studio.getStudioID()),
            name=sql.Literal(studio.getStudioName()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:  # Note - FOREIGN_KEY_VIOLATION not relevant here as it doesn't have one
        return ReturnValue.ERROR
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def deleteStudio(studio_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM StudioTable WHERE studio_id={id}").format(id=sql.Literal(studio_id))
        rows_effected, _ = conn.execute(query)
        conn.commit()  # TODO: Do we need it here or only rollback below?

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    studio = Studio()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From StudioTable WHERE studio_id={id}").format(id=sql.Literal(studio_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return studio.badStudio()
    except Exception:
        return studio.badStudio()

    finally:
        conn.close()
        if res.isEmpty():
            return studio.badStudio()
        else:
            studio.setStudioID(res.cols['studio_id'])
            studio.setStudioName(res.cols['name'])
    return studio


# ---------------------------------- BASIC API: ----------------------------------

def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: List[str]) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO CriticMovieRela VALUES({critic_id}, {movie_name}, {year}, {rating})").format(
            critic_id=sql.Literal(criticID),
            movie_name=sql.Literal(movieName),
            year=sql.Literal(movieYear),
            rating=sql.Literal(rating))

        conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:#TODO
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM CriticMovieRela WHERE critic_id={id} AND movie_name={name} AND year={year}").format(
            id=sql.Literal(criticID),
            name=sql.Literal(movieName),
            year=sql.Literal(movieYear))

        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    conn = None
    error_occurred = False
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("INSERT INTO ActorInMovieRela (movie_name, year, actor_id, salary) VALUES(\
                        {movie_name}, {year}, {actor_id}, {salary});\
                        INSERT INTO ActorRoleInMovieRela (movie_name, year, actor_id, role) VALUES").format(sql.Literal(movieName), sql.Literal(movieYear),
                                                                                                            sql.Literal(actorID), sql.Literal(salary))
        for i, role in enumerate(roles):
            query += sql.SQL("({movie_name}, {movie_year}, {actor_id}, {role})").format(sql.Literal(movieName), sql.Literal(movieYear),
                                                                                        sql.Literal(actorID), sql.Literal(role))
            if i != len(roles) - 1:  # if isn't the final role, need to add ',' between tuples
                query += sql.SQL(", ")

        rows_effected, _ = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        # TODO: check if roles is empty list
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:  # TODO (?)
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def actorDidntPlayInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM ActorInMovieRela WHERE movie_name={name} AND year={year} AND actor_id={id}").format(
            name=sql.Literal(movieName), year=sql.Literal(movieYear), id=sql.Literal(actorID))
        rows_effected, _ = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getActorsRoleInMovie(actor_id: int, movie_name: str, movieYear: int) -> List[str]:  # BONUS
    conn = None
    RolesList = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT roles FROM ActorRoleInMovieRela WHERE actor_id={id} AND  movie_name={movie} AND year={year} ORDER BY roles DESC").format(
            id=sql.Literal(actor_id),
            movie=sql.Literal(movie_name),
            year=sql.Literal(movieYear))

        rows_effected, res = conn.execute(query)
        # setting the output in a list
        for row in res.rows:
            RolesList.append(row[0])
    except DatabaseException:
        return []
    except Exception:
        return []
    finally:  # covers error of player not in movie / player or movie don't exist, because the query will return empty rows_received.
        conn.close()
    return RolesList


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO MovieInStudio VALUES({studio_id}, {movie_name}, {year}, {budget}, {revenue})").format(
            studio_id=sql.Literal(studioID),
            movie_name=sql.Literal(movieName),
            year=sql.Literal(movieYear),
            budget=sql.Literal(budget),
            revenue=sql.Literal(revenue))
        rows_effected, _ = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:  # TODO
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM MovieInStudio WHERE studio_name={studio_name} and movie_name={movie_name} and year={year}").format(
            studio_name=sql.Literal(studioID), movie_name=sql.Literal(movieName), year=sql.Literal(movieYear))
        rows_effected, _ = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    conn = None
    ret_val = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT rating FROM MovieAvgRate WHERE movie_name={movie_name} and year={year}").format(
            name=sql.Literal(movieName),
            year=sql.Literal(movieYear))
        rows_effected, ret_val = conn.execute(query)

        # case of division by 0
        if rows_effected == 0:
            res_val = 0

    except DatabaseException:
        res_val = 0
    except Exception:
        res_val = 0
    finally:
        conn.close()
    return ret_val

def averageActorRating(actorID: int) -> float:
    conn = None
    ret_val = 0
    try:
        # in words: in CriticMocieRela we filter the movies the actor played in. then, we group them by name and get avg
        # rating for each movie (as done in previous func). in the end, we return the avg of all movies avg rating.

        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(rating) FROM MovieAvgRate WHERE (movie_name, year)= " +
                        "(SELECT movie_name, year FROM ActorInMovieRela WHERE actor_id={id})").format(id=sql.Literal(actorID))

        rows_effected, ret_val = conn.execute(query)

        if rows_effected == 0:
            ret_val = 0
    except DatabaseException:
        res_val = 0
    except Exception:
        res_val = 0
    finally:
        conn.close()
    return ret_val


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    movie = Movie()
    try:
        # in words: in CriticMovieRela we filter the movies the actor played in. then, we group them by name and get avg
        # rating for each movie (as done in previous func). in the end, we order them by rating, year and name, and return movie with max rating.

        conn = Connector.DBConnector()
        query = sql.SQL("SELECT movie_name, year, MAX(rating) FROM MovieAvgRate WHERE (movie_name,year)=" +        #movie with max rating
                        "(SELECT movie_name, year FROM ActorInMovieRela WHERE actor_id={id})" +                     #movies actor palys in
                        " ORDER BY rating DESC, year ASC, movie_name DESC").format(id=sql.Literal(actor_id))
        rows_effected, res = conn.execute(query)
        assert len(res.rows) <= 1  # TODO: delete after testing

    except DatabaseException:
        return movie.badMovie()
    finally:
        conn.close()
        if res.isEmpty():
            return movie.badMovie()
        else:
            movie.setMovieName(res.cols['movie_name'])
            movie.setYear(res.cols['year'])
            movie.setGenre(res.cols['genre'])
    return movie

def stageCrewBudget(movieName: str, movieYear: int) -> int:  #TODO: I'm not sure if thats the right way to subtract
    conn = None
    ret_val = -1
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT budget - (SELECT SUM(salary) FROM ActorInMovieRela WHERE movie_name={name} AND year={year}) " +
                        "FROM MovieInStudio WHERE movie_name={name} AND year={year}").format(name=sql.Literal(movieName),
                                                                                             year=sql.Literal(movieYear))
        rows_effected, ret_val = conn.execute(query)
        if rows_effected == 0:  # movie not in studio
            ret_val = 0
    except DatabaseException:  # movie doesn't exist
        ret_val = -1
    finally:
        conn.close()
        return ret_val


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    conn = None
    ret_value = True
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM \
                        (SELECT movie_name, year, SUM(roles_num) AS sum_roles FROM NumRolesInMovieXActor WHERE movie_name={movie_name} AND year={year} AND actor_id<>{actor}) T1, \
                        (SELECT movie_name, year, SUM(roles_num) AS sum_roles FROM NumRolesInMovieXActor WHERE movie_name={movie_name} AND year={year} AND actor_id={actor}) T2 \
                        WHERE T1.not_actor_sum < T2.actor_sum")
        rows_effected, res = conn.execute(query)
        if res.isEmpty():  # TODO: should we check if value is None instead?
            ret_value = False
    except DatabaseException.UNKNOWN_ERROR as e:
        res_val = False
    except DatabaseException:
        res_val = False
    finally:
        conn.close()
    return ret_value


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    final_list = []
    try:
        conn = Connector.DBConnector()
        # To check: if I gave revenue default(0), will it give revenue 0 in the joined table for movies without one?
        query = sql.SQL("SELECT movie_name, COALESCE (revenue,0) FROM (SELECT * FROM MovieTable LEFT OUTER JOIN MovieRevenues) ORDER BY movie_name DESC")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(i)  # TODO: need to ensure it returns line i in format: (movie_name, revenues)
    #  TODO: do we need to handle errors?
    except DatabaseException as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list


def studioRevenueByYear() -> List[Tuple[int, int, int]]:
    conn = None
    final_list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM StudioRevenues ORDER BY studio_id DESC, year DESC")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])
    except DatabaseException as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list


def getFanCritics() -> List[Tuple[int, int]]:
    conn = None
    final_list = []
    try:
        # in words: doing inner join between CriticMovie to MovieInStudio to get matching movies. then, we grop the res
        # by critic_id and studio_id, and now if we count each group movies, we can tell how many movies from that studio
        # the critic rated. we can compare this num to the amount of movies the studio has. if equal - than he is fan.
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT critic_id, studio_id FROM CtiticToStudio WHERE (studio_id,movies_num)=(SELECT studio_id, COUNT(movie_name) FROM MovieInStudioRela GROUP BY studio_id)")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])

    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list


def averageAgeByGenre() -> List[Tuple[str, float]]:
    conn = None
    final_list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT genre, AVG(age) AS age_avg FROM ActorsInGenre LEFT OUTER JOIN ActorTable GROUP BY genre")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])

    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list



def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    final_list = []
    try:
        # in words: doing inner join between ActorInMovie to MovieInStudio to get matching movies. then, we group the res
        # by actor_id and studio_id, and now if we count each group movies, we can tell how many movies from that studio
        # the actor palyed in. we can compare this num to the amount of movies the actor played in. if equal - than he is exclusive.
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT actor_id, studio_id FROM ActorToStudio WHERE (actor_id,movies_num)= \
                        (SELECT actor_id, COUNT(movie_name) FROM ActorInMovieRela GROUP BY actor_id)")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])

    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list

# GOOD LUCK!
