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
            "DROP VIEW IF EXISTS MovieAvgRate CASCADE; CREATE VIEW MovieAvgRate AS SELECT movie_name, year, AVG(rating) FROM CriticMovieRela GROUP BY movie_name;"
            "CREATE TABLE ActorInMovieRela(movie_name PREFERANCE MovieTable ON DELETE CASCADE, year REFERANCES MovieTable ON DELETE CASCADE, actor_id PREFERANCE ActorTable ON DELETE CASCADE, salary INTEGER, CHECK(salary > 0), roles INTEGER NOT NULL, CHECK(all roles IS NOT NULL) );"
            "CREATE TABLE MovieInStudioRela(studio_it PREFERANCE StudioTable ON DELETE CASCADE, movie_name PREFERANCE MovieTable ON DELETE CASCADE, year REFERANCES MovieTable ON DELETE CASCADE, budget INTEGER, CHECK(budget >= 0), revenue INTEGER default(0), CHECK( revenue >= 0));"
            "DROP VIEW IF EXISTS MovieRevenues CASCADE; CREATE VIEW MovieRevenues AS SELECT studio_id, movie_name, SUM (revenue) FROM MovieInStudioRela GROUP BY movie_name;"
            "DROP VIEW IF EXISTS StudioRevenues CASCADE; CREATE VIEW StudioRevenues AS SELECT studio_id, movie_name, SUM (revenue) FROM MovieInStudioRela GROUP BY studio_id, year;"
            "DROP VIEW IF EXISTS CtiticToStudio CASCADE; CREATE VIEW CtiticToStudio AS SELECT critic_id, studio_id, COUNT (movie_name) AS movies_num FROM CriticMovieRela INNER JOIN MovieInStudioRela GROUP BY critic_id, studio_id;"
            "DROP VIEW IF EXISTS ActorToStudio CASCADE; CREATE VIEW ActorToStudio AS SELECT actor_id, studio_id, COUNT (movie_name) AS movies_num FROM ActorInMovieRela INNER JOIN MovieInStudioRela GROUP BY actor_id, studio_id;"
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
                     "DROP VIEW IF EXISTS MovieAvgRate CASCADE;"
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
    # TODO: Current implementation is problematic. we can't create getActorsRoleInMovie in the current build.
    # TODO: we need to have the option to select tuples based on each role, and
    #  if they are being saved as a list we can't do that (course staff won't allow).
    #  according to post @56 in the PIAZZA we need to some how do one of the following:
    #  1. change the ActorInMovieRela creation and adding query such that we inset multiple tubples in this 1 query
    #  (note that it means there cannot be unique attributes in ActorInMovieRela at all)
    #  OR 2. create 2 tables - 1) ActorInMovieRela which will not include the roles
    #                          2) ActorRoleInMovieRela which will only include actor_id, movie_name, year and *single*
    #                          role in each tupple.
    #  i tried to find how to implement the insert correctly (what they suggested in the post) but I'm not sure if I did
    #  it right.

    conn = None
    try:
        conn = Connector.DBConnector()
        rows = []
        values = ', '.join(map(str, rows))
        for i in roles:
            rows.insert((movieName, movieYear, actorID, salary, i))

        #query = sql.SQL(
        #    "INSERT INTO ActorInMovieRela VALUES({movie_name}, {year}, {actor_id}, {salary}, {roles})").format(
        #    movie_name=sql.Literal(movieName), year=sql.Literal(movieYear), actor_id=sql.Literal(actorID),
        #    salary=sql.Literal(salary), roles=sql.Literal(roles))
        query = sql.SQL("INSERT INTO ActorInMovieRela VALUES{}").format(', '.join(map(sql.Literal, rows))) #TODO: make sure its the right way to insert several tuples
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


def getActorsRoleInMovie(actor_id: int, movie_name: str, movieYear: int) -> List[str]: # TODO: implement after fixing ActorInMovieRela
    conn = None
    RolesList = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT roles FROM ActorInMovieRela WHERE actor_id == {id} AND  movie_name == {movie} ORDER BY roles DESC")
        rows_effected, rows_received = conn.execute(query)
        # setting the output in a list
        for i in rows_received.rows:
            RolesList.append(i[0])
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
            studio_name=sql.Literal(studioID), movie_name=sql.Literal(movieName), year=sql.Literal(movieYear))
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
    conn = None
    ret_val = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT rating FROM MovieAvgRate WHERE movie_name={movie_name} and year={year}").format(
            name=sql.Literal(movieName), year=sql.Literal(movieYear))
        rows_effected, ret_val = conn.execute(query)

        # case of division by 0
        if rows_effected == 0:
            res_val = 0

    except DatabaseException.CHECK_VIOLATION as e:
        res_val = 0
    except DatabaseException.UNKNOWN_ERROR as e:
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
    except DatabaseException.CHECK_VIOLATION as e:
        ret_val = 0
    except DatabaseException.UNKNOWN_ERROR as e:
        ret_val = 0
    finally:
        conn.close()
        return ret_val


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    bad_movie = False
    try:
        # in words: in CriticMocieRela we filter the movies the actor played in. then, we group them by name and get avg
        # rating for each movie (as done in previous func). in the end, we order them by rating, year and name, and return movie with max rating.

        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM MovieTable WHERE (movie_name, year)=" +
                        "(SELECT movie_name, year, MAX(rating) FROM MovieAvgRate WHERE (movie_name,year)=" +
                        "(SELECT movie_name, year FROM ActorInMovieRela WHERE actor_id={id})" +
                        " ORDER BY rating DESC, year ASC, movie_name DESC)").format(id=sql.Literal(actor_id))

        rows_effected, res = conn.execute(query)
    except DatabaseException.CHECK_VIOLATION as e:
        bad_movie = True
    finally:
        conn.close()
        if bad_movie is True:
            return Movie.badMovie()
        else:
            return createMovie(res)

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
    except DatabaseException.CHECK_VIOLATION as e:  # movie doesn't exist
        ret_val = -1
    finally:
        conn.close()
        return ret_val


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:  # TODO: implement after fixing ActorInMovieRela
    #conn = None
    #try:
       # conn = Connector.DBConnector()
       # query = sql.SQL("SELECT x FROM ActorInMovie T1, ActorInMovie T2 WHERE (SELECT COUNT(movie_name) FROM ActorInMovie WHERE movie_name={name}) < " +
       #                 "SELECT COUNT(movie_name) FROM ActorInMovie")

    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    final_list = []
    try:
        conn = Connector.DBConnector()
        # To check: if I gave revenue default(0), will it give revenue 0 in the joined table for movies without on?
        query = sql.SQL("SELECT movie_name, revenue FROM (SELECT * FROM MovieTable LEFT OUTER JOIN MovieRevenues) ORDER BY movie_name DESC")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])  # TODO: need to ensure it returns line i in format: (movie_name, revenues)
    #  TODO: do we need to handle errors?
    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list


def studioRevenueByYear() -> List[Tuple[str, int]]:
    conn = None
    final_list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM StudioRevenues ORDER BY studio_id DESC, year DESC")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])
    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
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
    # TODO: implement
    pass



def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    final_list = []
    try:
        # in words: doing inner join between ActorInMovie to MovieInStudio to get matching movies. then, we group the res
        # by actor_id and studio_id, and now if we count each group movies, we can tell how many movies from that studio
        # the actor palyed in. we can compare this num to the amount of movies the actor played in. if equal - than he is exclusive.
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT actor_id, studio_id FROM ActorToStudio WHERE (actor_id,movies_num)=(SELECT actor_id, COUNT(movie_name) FROM ActorInMovieRela GROUP BY actor_id)")
        rows_effected, res = conn.execute(query)

        for i in res.rows:
            final_list.append(res[i])

    except DatabaseException.UNKNOWN_ERROR as e:  # shouldn't get here
        print(e)
    finally:
        conn.close()
        return final_list

# GOOD LUCK!
