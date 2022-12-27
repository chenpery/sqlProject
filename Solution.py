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
            "BEGIN;" 
            # CriticTable
            "DROP TABLE IF EXISTS CriticTable CASCADE; " 
            "CREATE TABLE CriticTable(critic_id INTEGER NOT NULL, name TEXT NOT NULL," 
            "PRIMARY KEY(critic_id), CHECK(critic_id>0));" 
            # MovieTable
            "DROP TABLE IF EXISTS MovieTable CASCADE; " 
            "CREATE TABLE MovieTable(movie_name TEXT NOT NULL, year INTEGER NOT NULL, genre TEXT NOT NULL," 
            "PRIMARY KEY(movie_name, year), CHECK(year>= 1895), CHECK(genre = \'Drama\' OR genre=\'Action\' OR genre=\'Comedy\' OR genre=\'Horror\'));" 
            # ActorTable
            "DROP TABLE IF EXISTS ActorTable CASCADE; " 
            "CREATE TABLE ActorTable(actor_id INTEGER NOT NULL, name TEXT NOT NULL, age INTEGER NOT NULL, height INTEGER NOT NULL," 
            "PRIMARY KEY(actor_id), CHECK(actor_id > 0 AND age > 0 AND height > 0));" 
            # StudioTable
            "DROP TABLE IF EXISTS StudioTable CASCADE; " 
            "CREATE TABLE StudioTable(studio_id INTEGER NOT NULL, name TEXT NOT NULL," 
            "PRIMARY KEY(studio_id), CHECK(studio_id > 0));" 
            # CriticMovieRela
            "DROP TABLE IF EXISTS CriticMovieRela CASCADE; " 
            "CREATE TABLE CriticMovieRela(critic_id INTEGER NOT NULL, movie_name TEXT NOT NULL, year INTEGER NOT NULL, rating INTEGER NOT NULL," 
            "FOREIGN KEY(critic_id) REFERENCES CriticTable(critic_id) ON DELETE CASCADE," 
            "FOREIGN KEY(movie_name, year) REFERENCES MovieTable(movie_name, year) ON DELETE CASCADE," 
            "UNIQUE (critic_id, movie_name, year), CHECK(rating >=1 and rating <= 5));" 
            # ActorInMovieRela
            "DROP TABLE IF EXISTS ActorInMovieRela CASCADE; " 
            "CREATE TABLE ActorInMovieRela(movie_name TEXT NOT NULL, year INTEGER NOT NULL, actor_id INTEGER NOT NULL, salary INTEGER NOT NULL," 
            "FOREIGN KEY(actor_id) REFERENCES ActorTable(actor_id) ON DELETE CASCADE," 
            "FOREIGN KEY(movie_name,year) REFERENCES MovieTable(movie_name, year) ON DELETE CASCADE," 
            "PRIMARY KEY (movie_name,year,actor_id), CHECK(salary > 0));" 
            # ActorRoleInMovieRela
            "DROP TABLE IF EXISTS ActorRoleInMovieRela CASCADE; " 
            "CREATE TABLE ActorRoleInMovieRela(movie_name TEXT NOT NULL, year INTEGER NOT NULL, actor_id INTEGER NOT NULL, role TEXT NOT NULL," 
            "FOREIGN KEY(movie_name, year, actor_id) REFERENCES ActorInMovieRela(movie_name, year, actor_id) ON DELETE CASCADE," 
            "UNIQUE(movie_name, year, actor_id, role));" 
            # MovieInStudioRela
            "DROP TABLE IF EXISTS MovieInStudioRela CASCADE; " 
            "CREATE TABLE MovieInStudioRela(studio_id  INTEGER NOT NULL, movie_name TEXT NOT NULL, year INTEGER NOT NULL, budget INTEGER NOT NULL, revenue INTEGER NOT NULL," 
            "FOREIGN KEY(studio_id) REFERENCES StudioTable(studio_id) ON DELETE CASCADE," 
            "FOREIGN KEY(movie_name,year) REFERENCES MovieTable(movie_name, year) ON DELETE CASCADE," 
            "UNIQUE(movie_name,year), CHECK(budget >= 0), CHECK( revenue >= 0));" 
            # VIEWS
            "DROP VIEW IF EXISTS NumRolesInMovieXActor CASCADE; CREATE VIEW NumRolesInMovieXActor AS SELECT actor_id, movie_name, year, COUNT(role) AS roles_num FROM ActorRoleInMovieRela GROUP BY actor_id, movie_name, year;" 
            "DROP VIEW IF EXISTS MovieAvgRate CASCADE; CREATE VIEW MovieAvgRate AS SELECT movie_name, year, AVG(rating) AS rating_avg FROM CriticMovieRela GROUP BY movie_name, year;" 
            "DROP VIEW IF EXISTS MovieRevenues CASCADE; CREATE VIEW MovieRevenues AS SELECT movie_name, SUM(revenue) AS revenue FROM MovieInStudioRela GROUP BY movie_name;" 
            "DROP VIEW IF EXISTS StudioRevenues CASCADE; CREATE VIEW StudioRevenues AS SELECT studio_id, year, SUM(revenue) AS revenue FROM MovieInStudioRela GROUP BY studio_id, year;" 
            #"DROP VIEW IF EXISTS CtiticToStudio CASCADE; CREATE VIEW CtiticToStudio AS SELECT critic_id, studio_id, COUNT(movie_name) AS movies_num FROM CriticMovieRela INNER JOIN MovieInStudioRela ON (CriticMovieRela.movie_name,CriticMovieRela.year)=(MovieInStudioRela.movie_name,MovieInStudioRela.year) GROUP BY critic_id, studio_id;" +
            "DROP VIEW IF EXISTS CtiticToStudio CASCADE; CREATE VIEW CtiticToStudio AS SELECT critic_id, studio_id, COUNT(*) AS movies_num FROM CriticMovieRela INNER JOIN MovieInStudioRela ON (CriticMovieRela.movie_name,CriticMovieRela.year)=(MovieInStudioRela.movie_name,MovieInStudioRela.year) GROUP BY critic_id, studio_id;" 
            #"DROP VIEW IF EXISTS ActorToStudio CASCADE; CREATE VIEW ActorToStudio AS SELECT actor_id, studio_id, COUNT(movie_name) AS movies_num FROM ActorInMovieRela INNER JOIN MovieInStudioRela ON (ActorInMovieRela.movie_name,ActorInMovieRela.year)=(MovieInStudioRela.movie_name,MovieInStudioRela.year) GROUP BY actor_id, studio_id;" +
            "DROP VIEW IF EXISTS ActorToStudio CASCADE; CREATE VIEW ActorToStudio AS SELECT actor_id, studio_id, COUNT(*) AS movies_num FROM ActorInMovieRela INNER JOIN MovieInStudioRela ON (ActorInMovieRela.movie_name,ActorInMovieRela.year)=(MovieInStudioRela.movie_name,MovieInStudioRela.year) GROUP BY actor_id, studio_id;" 
            "DROP VIEW IF EXISTS ActorsInGenre CASCADE; CREATE VIEW ActorsInGenre AS SELECT DISTINCT actor_id, genre FROM ActorInMovieRela LEFT OUTER JOIN MovieTable ON (ActorInMovieRela.movie_name,ActorInMovieRela.year)=(MovieTable.movie_name,MovieTable.year);"
            "DROP VIEW IF EXISTS PMoviesSalary CASCADE; CREATE VIEW PMoviesSalary AS SELECT movie_name, year, SUM(salary) FROM ActorInMovieRela GROUP BY movie_name, year;"
            "COMMIT;"
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
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
                     "DROP VIEW IF EXISTS PMoviesSalary CASCADE;" +
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
    except DatabaseException.UNKNOWN_ERROR:
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


def createCritic(res: ResultSet) -> Critic:
    if res.size() == 0:
        return Critic.badCritic()
    critic_id = res[0]['critic_id']
    name = res[0]['name']
    return Critic(critic_id=critic_id, critic_name=name)


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From CriticTable WHERE critic_id={id}").format(id=sql.Literal(critic_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return Critic.badCritic()
    except Exception:
        return Critic.badCritic()
    finally:
        conn.close()
    return createCritic(res)


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
    except DatabaseException.UNKNOWN_ERROR:
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


def createActor(res: ResultSet) -> Actor:
    if res.size() == 0:
        return Actor.badActor()
    actor_id = res[0]['actor_id']
    name = res[0]['name']
    age = res[0]['age']
    height = res[0]['height']
    return Actor(actor_id=actor_id, actor_name=name, age=age, height=height)


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From ActorTable WHERE actor_id={id}").format(id=sql.Literal(actor_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return Actor.badActor()
    except Exception:
        return Actor.badActor()

    finally:
        conn.close()
    return createActor(res)


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
    except DatabaseException.UNKNOWN_ERROR:
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


def createMovie(res: ResultSet) -> Movie:
    if res.size() == 0:
        return Movie.badMovie()
    movie_name = res[0]['movie_name']
    year = res[0]['year']
    genre = res[0]['genre']
    return Movie(movie_name=movie_name, year=year,genre=genre)


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From MovieTable WHERE movie_name={name} and year={year}").format(
            name=sql.Literal(movie_name),
            year=sql.Literal(year))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return Movie.badMovie()
    except Exception:
        return Movie.badMovie()

    finally:
        conn.close()
    return createMovie(res)


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
    except DatabaseException.UNKNOWN_ERROR:
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

def createStudio(res: ResultSet) -> Studio:
    if res.size() == 0:
        return Studio.badStudio()
    studio_id = res[0]['studio_id']
    name = res[0]['name']
    return Studio(studio_id=studio_id, studio_name=name)


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From StudioTable WHERE studio_id={id}").format(id=sql.Literal(studio_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return Studio.badStudio()
    except Exception:
        return Studio.badStudio()

    finally:
        conn.close()
    return createStudio(res)


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
    except DatabaseException.FOREIGN_KEY_VIOLATION:
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
    try:
        conn = Connector.DBConnector()
        # TODO: check if roles is empty list
        query = sql.SQL("INSERT INTO ActorInMovieRela(movie_name, year, actor_id, salary) VALUES ( " 
                        "{movie_name}, {year}, {actor_id}, {salary});"
                        "INSERT INTO ActorRoleInMovieRela(movie_name, year, actor_id, role) VALUES ").format(movie_name=sql.Literal(movieName), year=sql.Literal(movieYear),
                                                                                                            actor_id=sql.Literal(actorID), salary=sql.Literal(salary))

        for i, role in enumerate(roles):
            query += sql.SQL("({movie_name}, {movie_year}, {actor_id}, {role})").format(movie_name=sql.Literal(movieName), movie_year=sql.Literal(movieYear),
                actor_id=sql.Literal(actorID), role=sql.Literal(role))

            if i is not (len(roles) -1):
                query += sql.SQL(", ")

        query += sql.SQL("; ")
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
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
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
            name=sql.Literal(movieName),
            year=sql.Literal(movieYear),
            id=sql.Literal(actorID))
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
            "SELECT role FROM ActorRoleInMovieRela WHERE actor_id={id} AND  movie_name={movie} AND year={year} ORDER BY role DESC").format(
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
            "INSERT INTO MovieInStudioRela VALUES({studio_id}, {movie_name}, {year}, {budget}, {revenue})").format(
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
    except DatabaseException.FOREIGN_KEY_VIOLATION:
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
            "DELETE FROM MovieInStudioRela WHERE studio_id={studio_id} and movie_name={movie_name} and year={year}").format(
            studio_id=sql.Literal(studioID), movie_name=sql.Literal(movieName), year=sql.Literal(movieYear))
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
        query = sql.SQL("SELECT rating_avg FROM MovieAvgRate WHERE movie_name={movie_name} and year={year}").format(
            movie_name=sql.Literal(movieName),
            year=sql.Literal(movieYear))
        rows_effected, res = conn.execute(query)

        if rows_effected and res[0]['rating_avg']:
            ret_val = res[0]['rating_avg']

    except DatabaseException:
        ret_val = 0
    except Exception:
        ret_val = 0
    finally:
        conn.close()
    return ret_val


def averageActorRating(actorID: int) -> float:
    conn = None
    ret_val = 0
    try:
        # in words: in CriticMovieRela we filter the movies the actor played in. then, we group them by name and get avg
        # rating for each movie (as done in previous func). in the end, we return the avg of all movies avg rating.
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(rating_avg) FROM MovieAvgRate INNER JOIN (SELECT movie_name, year FROM ActorInMovieRela WHERE actor_id={id}) AS table2"
                        " ON table2.year = MovieAvgRate.year and table2.movie_name= MovieAvgRate.movie_name; ").format(id=sql.Literal(actorID))

        rows_effected, res = conn.execute(query)

        if rows_effected and res[0]['avg']:
            ret_val = res[0]['avg']

    except DatabaseException:
        ret_val = 0
    except Exception as e:
        print(e)
        ret_val = 0
    finally:
        conn.close()
    return ret_val


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    try:
        # in words: in CriticMovieRela we filter the movies the actor played in. then, we group them by name and get avg
        # rating for each movie (as done in previous func). in the end, we order them by rating, year and name, and return movie with max rating.

        conn = Connector.DBConnector()
        query = sql.SQL("SELECT table2.movie_name, table2.year, genre, MAX(rating_avg) FROM "
                        "(SELECT table1.movie_name, table1.year, rating_avg From "
                        "(SELECT movie_name, year FROM actorinmovierela WHERE actor_id={id}) as table1 "
                        "INNER JOIN movieavgrate ON table1.movie_name=movieavgrate.movie_name and table1.year=movieavgrate.year) as table2 "
                        "INNER JOIN movietable ON movietable.movie_name=table2.movie_name and table2.year=movietable.year "
                        "GROUP BY table2.movie_name, table2.year, genre ORDER BY max DESC, table2.year ASC, table2.movie_name DESC LIMIT 1; ")\
            .format(id=sql.Literal(actor_id))
        rows_effected, res = conn.execute(query)

    except DatabaseException:
        return Movie.badMovie()
    finally:
        conn.close()
    return createMovie(res)

def stageCrewBudget(movieName: str, movieYear: int) -> int:
    conn = None
    ret_val = -1
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COALESCE(T2.budget - T1.salary, -1) AS final FROM "
                        "(SELECT MovieTable.movie_name, MovieTable.year, COALESCE(SUM(salary), 0) AS salary FROM MovieTable LEFT OUTER JOIN ActorInMovieRela ON (movieTable.movie_name, movieTable.year)=(ActorInMovieRela.movie_name, ActorInMovieRela.year) GROUP BY MovieTable.movie_name, MovieTable.year) T1,"
                        "(SELECT MovieTable.movie_name, MovieTable.year, COALESCE(budget, 0) AS budget FROM MovieTable LEFT OUTER JOIN MovieInStudioRela ON (movieTable.movie_name, movieTable.year)=(MovieInStudioRela.movie_name, MovieInStudioRela.year)) T2"
                        " WHERE (T1.movie_name,T1.year)=({name},{year}) AND (T2.movie_name,T2.year)=({name},{year})").format(name=sql.Literal(movieName), year=sql.Literal(movieYear))

        rows_effected, res = conn.execute(query)
        if res.size() < 1:  # movie not in studio
            ret_val = -1
        else:
            ret_val = res[0]['final']
    except Exception as e:  # movie doesn't exist
        print(e)
        ret_val = -1
    finally:
        conn.close()
        return ret_val


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT (SELECT COUNT(*) FROM ActorRoleInMovieRela WHERE movie_name={movie_name} and year={movie_year}) - "
                        "((SELECT COUNT(*) FROM ActorRoleInMovieRela WHERE"
                        " movie_name={movie_name} and year={movie_year} and actor_id={actor_id})*2) as overly")\
            .format(movie_name=sql.Literal(movie_name), movie_year=sql.Literal(movie_year), actor_id=sql.Literal(actor_id))
        rows_effected, res = conn.execute(query)

        if res[0]['overly'] < 0: #TODO : CHECK IF IT'S WORK FOR EQUAL
            return True
        else:
            return False

    except DatabaseException.UNKNOWN_ERROR as e:
        return False
    except DatabaseException:
        return False
    finally:
        conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    final_list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT T.movie_name, COALESCE(revenue,0) AS revenue FROM "
                        "(SELECT MovieTable.movie_name, revenue FROM MovieTable LEFT OUTER JOIN MovieRevenues ON (movieTable.movie_name)=(MovieRevenues.movie_name)) T ORDER BY movie_name DESC")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(i)

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
            final_list.append(i)
    except DatabaseException as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
    return final_list


def getFanCritics() -> List[Tuple[int, int]]:
    conn = None
    final_list = []
    try:
        # in words: doing inner join between CriticMovie to MovieInStudioRela to get matching movies. then, we grop the res
        # by critic_id and studio_id, and now if we count each group movies, we can tell how many movies from that studio
        # the critic rated. we can compare this num to the amount of movies the studio has. if equal - than he is fan.
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT critic_id, studio_id FROM CtiticToStudio WHERE (studio_id,movies_num) IN "
                        "(SELECT studio_id, COUNT(movie_name) FROM MovieInStudioRela GROUP BY studio_id) ORDER BY critic_id DESC, studio_id DESC")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(i)

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
        query = sql.SQL("SELECT genre, AVG(age) AS age_avg FROM ActorsInGenre LEFT OUTER JOIN ActorTable "
                        "ON (ActorsInGenre.actor_id)=(ActorTable.actor_id) GROUP BY genre")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(i)

    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
    return final_list



def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    final_list = []
    try:
        # in words: doing inner join between ActorInMovie to MovieInStudioRela to get matching movies. then, we group the res
        # by actor_id and studio_id, and now if we count each group movies, we can tell how many movies from that studio
        # the actor palyed in. we can compare this num to the amount of movies the actor played in. if equal - than he is exclusive.
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT actor_id, studio_id FROM ActorToStudio WHERE (actor_id,movies_num) IN  \
                        (SELECT actor_id, COUNT(movie_name) FROM ActorInMovieRela GROUP BY actor_id ORDER BY actor_id DESC )")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(i)

    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
    return final_list

# GOOD LUCK!
