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


# conn = Connector.DBConnector()

# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "CREATE TABLE CriticTable(critic_id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, CHECK ( critic_id > 0 ));"
            "CREATE TABLE MovieTable(movie_name TEXT NOT NULL PRIMARY KEY, year INTEGER NOT NULL PRIMARY KEY, genre TEXT NOT NULL,"
            " CHECK ( year >= 1895 ), CHECK ( genre = “Drama” or genre = “Action” or genre = “Comedy” or genre = “Horror”));"
            "CREATE TABLE ActorTable(actor_id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL, height INTEGER NOT NULL, "
            "CHECK( actor_id > 0 and age > 0 and height > 0 ));"
            "CREATE TABLE StudioTable(studio_name INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL,"
            " CHECK( id > 0 ));"
            "CREATE TABLE CriticMovieRela(critic_id REFERANCES CriticTable ON DELETE CASCADE, movie_name REFERANCES MovieTable ON DELETE CASCADE, year REFERANCES MovieTable ON DELETE CASCADE, rating INTEGER, CHECK( rating >=1 and rating <= 5));"
            "CREATE TABLE ActorInMovieRela(movie_name PREFERANCE MovieTable ON DELETE CASCADE, year REFERANCES MovieTable ON DELETE CASCADE, actor_id PREFERANCE ActorTable ON DELETE CASCADE, salary INTEGER, CHECK(salary > 0), roles INTEGER[] NOT NULL, CHECK(all roles IS NOT NULL) );"
            "CREATE TABLE MovieInStudioRela(studio_it PREFERANCE StudioTable ON DELETE CASCADE, movie_name PREFERANCE MovieTable ON DELETE CASCADE, year REFERANCES MovieTable ON DELETE CASCADE, budget INTEGER, CHECK(budget >= 0), revenue INTEGER, CHECK( revenue >= 0));"
            "COMMIT;"
        )
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM CriticTable;"
                     "DELETE FROM MovieTable;"
                     "DELETE FROM ActorTable;"
                     "DELETE FROM StudioTable;"
                     "DELETE FROM CriticMovieRela;"
                     "DELETE FROM ActorInMovieRela;"
                     "DELETE FROM MovieInStudioRela;"
                     )
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE CriticTable;"
                     "DROP TABLE MovieTable;"
                     "DROP TABLE ActorTable;"
                     "DROP TABLE StudioTable;"
                     "DROP TABLE CriticMovieRela;"
                     "DROP TABLE ActorInMovieRela;"
                     "DROP TABLE MovieInStudioRela;"
                     )
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        assert isinstance(critic, Critic)
        query = sql.SQL("INSERT INTO CriticTable VALUES({critic_id}, {name})").format(
            critic_id=sql.Literal(critic.getCriticID()), name=sql.Literal(critic.getName()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION: #TODO
        return ReturnValue.ALREADY_EXISTS
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
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def createCritic(res: ResultSet) -> Critic:
    if res.size() == 0:
        return Critic.badCritic()
    critic_id = res.cols['critic_id']
    name = res.cols['name']
    return Critic(critic_id=critic_id, critic_name=name)


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From CriticTable WHERE critic_id={id}").format(id=sql.Literal(critic_id))
        rows_effected, res = conn.execute(query)
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
            actor_id=sql.Literal(Actor.getActorID()), name=sql.Literal(Actor.getName()), age=sql.Literal(Actor.getAge()), height=sql.Literal(Actor.getHeight()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION: #TODO
        return ReturnValue.ALREADY_EXISTS
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
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def createActor(res: ResultSet) -> Actor:
    if res.size() == 0:
        return Actor.badActor()
    actor_id = res.cols['actor_id']
    name = res.cols['name']
    age = res.cols['age']
    height = res.cols['height']
    return Actor(actor_id=actor_id, actor_name=name, age=age, height=height)


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From ActorTable WHERE actor_id={id}").format(id=sql.Literal(actor_id))
        rows_effected, res = conn.execute(query)
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
            name=sql.Literal(Movie.getMovieName()), year=sql.Literal(Movie.getYear()), genre=sql.Literal(Movie.getGenre()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:#TODO
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM MovieTable WHERE movie_name={name} and year={year}").format(name=sql.Literal(movie_name), year=sql.Literal(year))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def createMovie(res: ResultSet) -> Movie:
    if res.size() == 0:
        return Movie.badMovie()
    movie_name = res.cols['movie_name']
    year = res.cols['year']
    genre = res.cols['genre']
    return Movie(movie_name=movie_name, year=year,genre=genre)


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From MovieTable WHERE name={name} and year={year}").format(name=sql.Literal(movie_name), year=sql.Literal(year))
        rows_effected, res = conn.execute(query)
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
            studio_id=sql.Literal(studio.setStudioID()), name=sql.Literal(studio.getName()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:#TODO
        return ReturnValue.ALREADY_EXISTS
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
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def createStudio(res: ResultSet) -> Studio:
    if res.size() == 0:
        return Studio.badStudio()
    studio_id = res.cols['studio_id']
    name = res.cols['name']
    return Studio(studio_id=studio_id, studio_name=name)


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * From StudioTable WHERE studio_id={id}").format(id=sql.Literal(studio_id))
        rows_effected, res = conn.execute(query)
    except Exception:
        return Studio.badStudio()
    finally:
        conn.close()
    return createStudio(res)


# ---------------------------------- BASIC API: ----------------------------------

def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO CriticMovieRela VALUES({critic_id}, {movie_name}, {year}, {rating})").format(
            critic_id=sql.Literal(criticID), movie_name=sql.Literal(movieName), year=sql.Literal(movieYear), rating=sql.Literal(rating))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:#TODO
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
        query = sql.SQL("DELETE FROM CriticMovieRela WHERE critic_id={id} and movie_name={name} and year={year}").format(
            id=sql.Literal(criticID), name=sql.Literal(movieName), year=sql.Literal(movieYear))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue: #TODO: NOTE ?
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO ActorInMovieRela VALUES({movie_name}, {year}, {actor_id}, {salary}, {roles})").format(
            movie_name=sql.Literal(movieName), year=sql.Literal(movieYear), actor_id=sql.Literal(actorID), salary=sql.Literal(salary), roles=sql.Literal(roles))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:#TODO
        return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM ActorInMovieRela WHERE movie_name={name} and year={year} and actor_id={id}").format(
            name=sql.Literal(movieName), year=sql.Literal(movieYear), id=sql.Literal(actorID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getActorsRoleInMovie(actor_id: int, movie_name: str, movieYear: int) -> List[str]:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO MovieInStudio VALUES({studio_id}, {movie_name}, {year}, {budget}, {revenue})").format(
            studio_id=sql.Literal(studioID), movie_name=sql.Literal(movieName), year=sql.Literal(movieYear),
            budget=sql.Literal(budget), revenue=sql.Literal(revenue))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:  # TODO
        return ReturnValue.NOT_EXISTS
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
            studio_name=sql.Literal(studioID), movie_name=sql.Literal(movieName), year=sql.Literal(year))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def studioRevenueByYear() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass

# GOOD LUCK!
