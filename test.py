
from Solution import *


if __name__ == '__main__':

    dropTables()
    createTables()
    clearTables()
    genres = ["Drama", "Action", "Comedy", "Horror"]



    print("########## TEST AddActor BEGIN #################")
    for i in range(1, 10):
        assert addActor(Actor(i, f'actor_{i}', i * 10, i * 10)) == ReturnValue.OK
    assert addActor(Actor(1, f'actor', 10, 10)) == ReturnValue.ALREADY_EXISTS
    assert addActor(Actor(10, f'actor', 0, 10)) == ReturnValue.BAD_PARAMS
    assert addActor(Actor(10, f'actor', 10, -1)) == ReturnValue.BAD_PARAMS
    print("########## TEST AddActor END #################\n")

    print("########## TEST GetActor BEGIN #################")
    for i in range(1, 7):
        assert getActorProfile(i) == Actor(i, f'actor_{i}',i * 10, i * 10)

    assert getActorProfile(100) == Actor.badActor()
    print("########## TEST GetActor END #################\n")


    print("########## TEST DeleteActor BEGIN #################")
    for i in range(7, 10):
        assert deleteActor(i) == ReturnValue.OK

    assert deleteActor(1000) == ReturnValue.NOT_EXISTS
    print("########## TEST DeleteActor END #################\n" )



    print("########## TEST AddCritic BEGIN #################")
    for i in range(1, 10):
        assert addCritic(Critic(i, f'critic_{i}')) == ReturnValue.OK

    assert addCritic(Critic(1, f'critic')) == ReturnValue.ALREADY_EXISTS
    print("########## TEST AddCritic END #################\n")



    print("########## TEST GetCritic BEGIN #################")
    for i in range(1, 7):
        assert getCriticProfile(i) == Critic(i, f'critic_{i}')

    assert getCriticProfile(100) == Critic.badCritic()
    print("########## TEST GetCritic END #################\n")



    print("########## TEST DeleteCritic BEGIN #################")
    for i in range(7, 10):
        assert deleteCritic(i) == ReturnValue.OK

    assert deleteCritic(1000) == ReturnValue.NOT_EXISTS
    print("########## TEST DeleteCritic END #################\n")


    print("########## TEST AddStudio BEGIN #################")
    for i in range(1, 10):
        assert addStudio(Studio(i, f'studio_{i}')) == ReturnValue.OK

    assert addStudio(Studio(1, f'studio')) == ReturnValue.ALREADY_EXISTS
    print("########## TEST AddStudio END #################\n")


    print("########## TEST GetStudio BEGIN #################")
    for i in range(1, 7):
        assert getStudioProfile(i) == Studio(i, f'studio_{i}')

    assert getStudioProfile(100) == Studio.badStudio()
    print("########## TEST GetStudio END #################\n")



    print("########## TEST DeleteStudio BEGIN #################")
    for i in range(7, 10):
        assert deleteStudio(i) == ReturnValue.OK

    assert deleteStudio(1000) == ReturnValue.NOT_EXISTS
    print("########## TEST DeleteStudio END #################\n")


    print("########## TEST AddMovie BEGIN #################")
    for i in range(1, 10):
        assert addMovie(Movie(f"movie_{i}", 2000 + i, genres[i%4])) == ReturnValue.OK

    assert addMovie(Movie(f"movie_{2}", 2000 + 1, genres[2])) == ReturnValue.OK
    assert addMovie(Movie(f"movie_{1}", 2000 + 2, genres[1])) == ReturnValue.OK
    assert addMovie(Movie(f"movie_{10}", 2000 + 1, genres[2])) == ReturnValue.OK
    assert addMovie(Movie(f"movie_{1}", 2000 + 10, genres[1])) == ReturnValue.OK

    assert addMovie(Movie(f"movie_{1}", 2001, genres[1])) == ReturnValue.ALREADY_EXISTS
    assert addMovie(Movie(f"movie_{11}", 2011, "Docu")) == ReturnValue.BAD_PARAMS
    assert addMovie(Movie(f"movie_{11}", 1000, "Drama")) == ReturnValue.BAD_PARAMS
    assert addMovie(Movie(f"movie_{11}", 1000, "Docu")) == ReturnValue.BAD_PARAMS
    print("########## TEST AddMovie END #################\n")



    print("########## TEST GetMovie BEGIN #################")
    genres = ["Drama", "Action", "Comedy", "Horror"]
    for i in range(1, 7):
        assert getMovieProfile(f"movie_{i}", 2000 + i) == Movie(f"movie_{i}", 2000 + i, genres[i%4])

    assert getMovieProfile("Bad Movie", 2345) == Movie.badMovie()
    print("########## TEST GetMovie END #################\n")

    print("########## TEST DeleteMovie BEGIN #################")
    genres = ["Drama", "Action", "Comedy", "Horror"]
    for i in range(7, 10):
        assert deleteMovie(f"movie_{i}", 2000 + i) == ReturnValue.OK

    assert deleteMovie(f"movie_{2}", 2000 + 1) == ReturnValue.OK
    assert deleteMovie(f"movie_{1}", 2000 + 2) == ReturnValue.OK
    assert deleteMovie(f"movie_{10}", 2000 + 1) == ReturnValue.OK
    assert deleteMovie(f"movie_{1}", 2000 + 10) == ReturnValue.OK

    assert deleteMovie(f"movie_{10}", 2000 + 1) == ReturnValue.NOT_EXISTS
    assert deleteMovie(f"movie_{1}", 2000 + 10) == ReturnValue.NOT_EXISTS
    print("########## TEST DeleteMovie END #################\n")


    print("########## TEST CriticRatedMovie BEGIN #################")
    for i in range(1,7):
        assert criticRatedMovie(f"movie_{i}", 2000 + i, i, (i % 5) + 1 ) == ReturnValue.OK

    assert criticRatedMovie(f"movie_1", 2001, 2, 0) == ReturnValue.BAD_PARAMS
    assert criticRatedMovie(f"movie_1", 2001, 2, 10) == ReturnValue.BAD_PARAMS
    assert criticRatedMovie(f"movie_1", 2001, 2, -12) == ReturnValue.BAD_PARAMS

    assert criticRatedMovie(f"movie_1", 2001, 1, 1) == ReturnValue.ALREADY_EXISTS

    assert criticRatedMovie(f"movie_no", 2001, 1, 1) == ReturnValue.NOT_EXISTS
    assert criticRatedMovie(f"movie_1", 2020, 1, 1) == ReturnValue.NOT_EXISTS
    assert criticRatedMovie(f"movie_1", 2001, 20, 1) == ReturnValue.NOT_EXISTS

    assert criticRatedMovie(f"movie_1", 2001, 2, 1) == ReturnValue.OK
    assert criticRatedMovie(f"movie_2", 2002, 1, 1) == ReturnValue.OK
    print("########## TEST CriticRatedMovie END #################\n")


    print("########## TEST CriticDidntRatedMovie Begin #################")
    assert criticDidntRateMovie(f"movie_1", 2001, 2) == ReturnValue.OK
    assert criticDidntRateMovie(f"movie_2", 2002, 1) == ReturnValue.OK

    assert criticDidntRateMovie(f"movie_2", 2002, 1) == ReturnValue.NOT_EXISTS
    assert criticDidntRateMovie(f"movie_10", 2001, 1) == ReturnValue.NOT_EXISTS
    assert criticDidntRateMovie(f"movie_1", 2001, 15) == ReturnValue.NOT_EXISTS
    print("########## TEST CriticDidntRatedMovie END #################\n")


    print("########## TEST actorPlayedInMovie BEGIN #################")
    for i in range(1, 7):
        assert actorPlayedInMovie(f"movie_{i}", 2000 + i, i, i * 1000, [f"role_{i}"]) == ReturnValue.OK

    assert actorPlayedInMovie('movie_1', 2001, 2, -1000, ['role']) == ReturnValue.BAD_PARAMS
    assert actorPlayedInMovie('movie_1', 2001, 2, 0,['role']) == ReturnValue.BAD_PARAMS
    assert actorPlayedInMovie('movie_1', 2001, 2, -1000, ['role', None]) == ReturnValue.BAD_PARAMS
    assert actorPlayedInMovie('movie_1', 2001, 2, 0, []) == ReturnValue.BAD_PARAMS



    assert actorPlayedInMovie('movie_1', 2001, 10, 1000, ['role']) == ReturnValue.NOT_EXISTS
    assert actorPlayedInMovie('movie', 2001, 1, 1000, ['role']) == ReturnValue.NOT_EXISTS
    assert actorPlayedInMovie('movie_1', 2000, 1, 1000, ['role']) == ReturnValue.NOT_EXISTS
    assert actorPlayedInMovie('movie', 2000, 10, 1000, ['role']) == ReturnValue.NOT_EXISTS
    assert actorPlayedInMovie('movie', 2001, 10, 1000, ['role']) == ReturnValue.NOT_EXISTS

    assert actorPlayedInMovie('movie_2', 2002, 1, 1000, ['role']) == ReturnValue.OK
    assert actorPlayedInMovie('movie_1', 2001, 2, 1000, ['role']) == ReturnValue.OK

    assert actorPlayedInMovie('movie_2', 2002, 1, 1000, ['role_bis']) == ReturnValue.ALREADY_EXISTS


    print("########## TEST actorPlayedInMovie END #################\n")


    print("########## TEST actorDidntPlayedInMovie BEGIN #################")
    assert actorDidntPlayInMovie('movie_2', 2002, 1) == ReturnValue.OK
    assert actorDidntPlayInMovie('movie_1', 2001, 2) == ReturnValue.OK

    assert actorDidntPlayInMovie('movie_1', 2001, 2) == ReturnValue.NOT_EXISTS
    assert actorDidntPlayInMovie('movie_20', 2001, 1) == ReturnValue.NOT_EXISTS
    print("########## TEST actorDidntPlayedInMovie END #################\n")



    print("########## TEST StudioProducedMovie BEGIN #################")
    for i in range(1,7):
        assert studioProducedMovie(i,f"movie_{i}", 2000 + i, i * 100000, i * 1000000) == ReturnValue.OK

    assert studioProducedMovie(2, f"movie_{1}", 2000 + 1, -100000, 1000000) == ReturnValue.BAD_PARAMS
    assert studioProducedMovie(2, f"movie_{1}", 2000 + 1, 100000, -1000000) == ReturnValue.BAD_PARAMS
    assert studioProducedMovie(2, f"movie_{1}", 2000 + 1,-1, 1000000) == ReturnValue.BAD_PARAMS


    assert studioProducedMovie(1, f"movie_{1}", 2000 + 1, 100000, 1000000) == ReturnValue.ALREADY_EXISTS
    assert studioProducedMovie(100, f"movie_{1}", 2000 + 1, 100000, 1000000) == ReturnValue.ALREADY_EXISTS

    assert studioProducedMovie(1, f"movie_{100}", 2000 + 1, 100000, 1000000) == ReturnValue.NOT_EXISTS
    assert studioProducedMovie(100, f"movie_{100}", 2000 + 100, 100000, 1000000) == ReturnValue.NOT_EXISTS

    assert studioProducedMovie(1, f"movie_{2}", 2000 + 2, 100000, 1000000) == ReturnValue.ALREADY_EXISTS
    assert studioProducedMovie(2, f"movie_{1}", 2000 + 1, 100000, 1000000) == ReturnValue.ALREADY_EXISTS

    print("########## TEST StudioProducedMovie END #################\n")

    print("########## TEST StudioDidntProducedMovie BEGIN #################")
    for i in [1,2]:
        assert studioDidntProduceMovie(i, f"movie_{i}", 2000 + i) == ReturnValue.OK
    for i in [1,2]:
        assert studioDidntProduceMovie(i, f"movie_{i}", 2000 + i) == ReturnValue.NOT_EXISTS

    assert studioDidntProduceMovie(2, f"movie_{1}", 2001) == ReturnValue.NOT_EXISTS

    for i in range(1,2):
        assert studioProducedMovie(i,f"movie_{i}", 2000 + i, i * 100000, i * 1000000) == ReturnValue.OK
    print("########## TEST StudioDidntProducedMovie END #################\n")


    print("########## TEST AverageRating BEGIN #################")

    # Simple test
    for i in range(1, 7):
        assert averageRating(f'movie_{i}', 2000 + i) == (i % 5) + 1

    assert averageRating(f'error', 2000) == 0
    assert averageRating(f'movie_1', 2010) == 0

    addMovie(Movie("test", 2000, "Drama"))

    assert averageRating("test", 2000) == 0

    criticRatedMovie("test", 2000,1, 1)
    criticRatedMovie("test", 2000,2, 2)
    criticRatedMovie("test", 2000, 3, 3)
    assert averageRating("test", 2000) == 2
    criticDidntRateMovie("test", 2000, 1)
    assert averageRating("test", 2000) == 2.5
    criticDidntRateMovie("test", 2000, 3)
    assert averageRating("test", 2000) == 2
    criticDidntRateMovie("test", 2000, 2)
    assert averageRating("test", 2000) == 0
    deleteMovie("test", 2000)

    print("########## TEST AverageRating END #################\n")
    print("########## TEST AverageActorRating BEGIN #################")

    # Simple test
    for i in range(1, 7):
        assert averageActorRating(i) == (i % 5) + 1

    assert averageActorRating(10) == 0 #Actor 10 doesn't exists
    addActor(Actor(10, "actor_temp", 10, 10))

    assert averageActorRating(10) == 0 # Actor 10 doesn't play in any movie
    addCritic(Critic(10, 'critic_temp'))
    addCritic(Critic(11, 'critic_temp'))
    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))

    actorPlayedInMovie('movie_temp_1', 2000,10, 1000,['role_1_1', 'role_1_2'])
    actorPlayedInMovie('movie_temp_2', 2000,10, 1000,['role_2_1', 'role_2_2', 'role_2_3'])

    criticRatedMovie('movie_temp_1', 2000,10, 5)
    criticRatedMovie('movie_temp_2', 2000, 10, 4)

    res = averageActorRating(10)
    assert averageActorRating(10) == 4.5

    criticRatedMovie('movie_temp_1', 2000, 11, 3)
    assert averageActorRating(10) == 4
    criticDidntRateMovie('movie_temp_1', 2000, 11)
    actorDidntPlayInMovie('movie_temp_2', 2000, 10)
    assert averageActorRating(10) == 5
    actorDidntPlayInMovie('movie_temp_1', 2000,10)
    assert averageActorRating(10) == 0

    deleteActor(10)
    deleteCritic(10)
    deleteCritic(11)
    deleteMovie('movie_temp_1', 2000)
    deleteMovie('movie_temp_2', 2000)

    print("########## TEST AverageActorRating END #################\n")

    print("########## TEST BestPerformance BEGIN #################")
    for i in range(1, 7):
        assert bestPerformance(i) == Movie(f"movie_{i}", 2000 + i, genres[i%4])


    addActor(Actor(10, "actor_temp", 10, 10))

    assert bestPerformance(10) == Movie.badMovie()

    addCritic(Critic(10, 'critic_temp'))
    addCritic(Critic(11, 'critic_temp'))
    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))

    actorPlayedInMovie('movie_temp_1', 2000, 10, 1000, ['role_1', 'role_2'])
    actorPlayedInMovie('movie_temp_2', 2000, 10, 1000, ['role_1', 'role_2'])
    assert bestPerformance(10) == Movie.badMovie()

    criticRatedMovie('movie_temp_1', 2000, 10, 5)
    assert bestPerformance(10) == Movie('movie_temp_1', 2000, "Drama")

    criticRatedMovie('movie_temp_2', 2000, 10, 3)
    assert bestPerformance(10) == Movie('movie_temp_1', 2000, "Drama")

    criticDidntRateMovie('movie_temp_1', 2000, 10)
    assert bestPerformance(10) == Movie('movie_temp_2', 2000, "Drama")

    criticRatedMovie('movie_temp_1', 2000, 11, 4)
    assert bestPerformance(10) == Movie('movie_temp_1', 2000, "Drama")

    criticDidntRateMovie('movie_temp_2', 2000, 10)
    criticDidntRateMovie('movie_temp_1', 2000, 11)

    assert bestPerformance(10) == Movie.badMovie()

    deleteActor(10)
    deleteCritic(10)
    deleteCritic(11)
    deleteMovie('movie_temp_1', 2000)
    deleteMovie('movie_temp_2', 2000)

    print("########## TEST BestPerformance END #################\n")

    print("########## TEST StageCrewBudget BEGIN #################")

    addActor(Actor(10, "actor_temp", 10, 10))
    addActor(Actor(11, "actor_temp", 10, 10))
    addStudio(Studio(10, 'studio_temp'))

    assert stageCrewBudget('movie_temp_1', 2000) == -1
    addMovie(Movie('movie_temp_1', 2000, "Drama"))

    assert stageCrewBudget('movie_temp_1', 2000) == 0
    studioProducedMovie(10, 'movie_temp_1', 2000, 10000, 1)
    assert stageCrewBudget('movie_temp_1', 2000) == 10000

    actorPlayedInMovie('movie_temp_1', 2000, 10, 1000, ['r1', 'r2'])
    assert stageCrewBudget('movie_temp_1', 2000) == 9000

    actorPlayedInMovie('movie_temp_1', 2000, 11, 500, ['r1', 'r2'])
    assert stageCrewBudget('movie_temp_1', 2000) == 8500

    deleteActor(10)

    assert stageCrewBudget('movie_temp_1', 2000) == 9500
    deleteActor(11)

    deleteStudio(10)
    assert stageCrewBudget('movie_temp_1', 2000) == 0
    deleteMovie('movie_temp_1', 2000)
    print("########## TEST StageCrewBudget END #################\n")


    print("########## TEST OverlyInvested BEGIN #################")

    assert not overlyInvestedInMovie('movie_temp_1', 2000,10)
    addActor(Actor(10, "actor_temp", 10, 10))
    assert not overlyInvestedInMovie('movie_temp_1', 2000, 10)
    addActor(Actor(11, "actor_temp", 10, 10))
    addActor(Actor(12, "actor_temp", 10, 10))

    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))
    addMovie(Movie('movie_temp_3', 2000, "Drama"))

    actorPlayedInMovie('movie_temp_1', 2000, 10, 1000, ['r1', 'r2'])
    actorPlayedInMovie('movie_temp_1', 2000, 11, 1000, ['r2'])

    assert overlyInvestedInMovie('movie_temp_1', 2000, 10)
    assert not overlyInvestedInMovie('movie_temp_1', 2000, 11)
    assert not overlyInvestedInMovie('movie_temp_1', 2000, 12)

    actorPlayedInMovie('movie_temp_2', 2000, 10, 1000, ['r1', 'r2'])
    actorPlayedInMovie('movie_temp_2', 2000, 11, 1000, ['r2', 'r3', 'r4'])

    assert not overlyInvestedInMovie('movie_temp_2', 2000, 10)
    assert overlyInvestedInMovie('movie_temp_2', 2000, 11)

    actorPlayedInMovie('movie_temp_3', 2000, 10, 1000, ['r1', 'r2'])
    actorPlayedInMovie('movie_temp_3', 2000, 11, 1000, ['r2'])
    actorPlayedInMovie('movie_temp_3', 2000, 12, 1000, ['r2', 'r3', 'r4', 'r5'])

    assert not overlyInvestedInMovie('movie_temp_3', 2000, 10)
    assert not overlyInvestedInMovie('movie_temp_3', 2000, 11)

    print("########## TEST OverlyInvested BEGIN ################# \n")

    print("########## TEST Franchise Revenue BEGIN #################")

    clearTables()
    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_1', 2005, "Drama"))

    addMovie(Movie('movie_temp_2', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2005, "Drama"))

    addStudio(Studio(10, 'studio_temp'))
    addStudio(Studio(11, 'studio_temp'))
    addStudio(Studio(12, 'studio_temp'))

    assert franchiseRevenue() == [('movie_temp_2', 0), ('movie_temp_1', 0)]
    studioProducedMovie(10,'movie_temp_1', 2000,1, 1)

    assert franchiseRevenue() == [('movie_temp_2', 0), ('movie_temp_1', 1)]

    studioProducedMovie(11, 'movie_temp_1',2005,1, 1 )
    assert franchiseRevenue() == [('movie_temp_2', 0), ('movie_temp_1', 2)]

    studioProducedMovie(12, 'movie_temp_2', 2000, 1, 1)
    assert franchiseRevenue() == [('movie_temp_2', 1), ('movie_temp_1', 2)]

    studioProducedMovie(10, 'movie_temp_2', 2005, 1, 1)
    assert franchiseRevenue() == [('movie_temp_2', 2), ('movie_temp_1', 2)]

    studioDidntProduceMovie(10, 'movie_temp_2', 2005)
    assert franchiseRevenue() == [('movie_temp_2', 1), ('movie_temp_1', 2)]

    deleteMovie('movie_temp_1', 2000)
    deleteMovie('movie_temp_1', 2005)

    assert franchiseRevenue() == [('movie_temp_2', 1)]

    deleteMovie('movie_temp_2', 2000)
    deleteMovie('movie_temp_2', 2005)

    deleteStudio(10)
    deleteStudio(11)
    deleteStudio(12)

    assert franchiseRevenue() == []
    print("########## TEST FranchiseRevenue END ################# \n")

    print("########## TEST Studio Revenue By Year BEGIN ################# ")
    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_1', 2005, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2005, "Drama"))
    addMovie(Movie('movie_temp_3', 2003, "Drama"))


    addStudio(Studio(10, 'studio_temp'))
    addStudio(Studio(11, 'studio_temp'))
    addStudio(Studio(12, 'studio_temp'))

    assert studioRevenueByYear() == []

    studioProducedMovie(10,'movie_temp_1', 2000,1, 1)
    assert studioRevenueByYear() == [(10, 2000, 1)]

    studioProducedMovie(10,'movie_temp_2', 2000, 1, 1)
    studioProducedMovie(11, 'movie_temp_3', 2003, 1, 1)

    assert studioRevenueByYear() == [(11,2003, 1), (10, 2000, 2)]

    studioProducedMovie(10,'movie_temp_2', 2005, 1, 1)
    assert studioRevenueByYear() == [(11, 2003, 1), (10, 2005, 1),(10, 2000, 2)]

    studioProducedMovie(12, 'movie_temp_1', 2005, 1, 1)
    assert studioRevenueByYear() == [(12, 2005, 1),(11, 2003, 1), (10, 2005, 1),(10, 2000, 2)]

    studioDidntProduceMovie(10, 'movie_temp_2', 2005)
    assert studioRevenueByYear() == [(12, 2005, 1), (11, 2003, 1), (10, 2000, 2)]

    deleteStudio(11)

    assert studioRevenueByYear() == [(12, 2005, 1), (10, 2000, 2)]

    deleteMovie('movie_temp_1', 2000)
    assert studioRevenueByYear() == [(12, 2005, 1), (10, 2000, 1)]

    deleteStudio(12)
    deleteStudio(10)

    deleteMovie('movie_temp_1', 2005)
    deleteMovie('movie_temp_2', 2005)
    deleteMovie('movie_temp_2', 2000)
    deleteMovie('movie_temp_3', 2003)

    print("########## TEST Studio Revenue By Year END #################\n")
    print("########## TEST Get Fan Critic BEGIN #################")

    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))
    addMovie(Movie('movie_temp_3', 2000, "Drama"))
    addMovie(Movie('movie_temp_4', 2000, "Drama"))
    addMovie(Movie('movie_temp_5', 2000, "Drama"))

    addStudio(Studio(1, 'studio_temp_1'))
    addStudio(Studio(2, 'studio_temp_2'))
    addStudio(Studio(3, 'studio_temp_3'))

    addCritic(Critic(1, 'critic_temp_1'))
    addCritic(Critic(2, 'critic_temp_1'))
    addCritic(Critic(3, 'critic_temp_1'))

    assert getFanCritics() == []
    studioProducedMovie(1, 'movie_temp_1', 2000,1, 1)
    assert getFanCritics() == []
    criticRatedMovie('movie_temp_1', 2000, 1,5)
    assert getFanCritics() == [(1,1)]
    studioProducedMovie(1,'movie_temp_2', 2000, 1, 1)
    assert getFanCritics() == []
    studioProducedMovie(2, 'movie_temp_3', 2000, 1, 1)

    criticRatedMovie('movie_temp_3', 2000, 2,5)
    criticRatedMovie('movie_temp_3', 2000, 1, 5)


    assert getFanCritics() == [(2,2), (1, 2)]

    studioProducedMovie(2, 'movie_temp_4', 2000, 1, 1)
    criticRatedMovie('movie_temp_4', 2000, 2, 5)

    assert getFanCritics() == [(2, 2)]

    deleteMovie('movie_temp_2', 2000)

    assert getFanCritics() == [(2, 2), (1,1)]

    studioProducedMovie(3, 'movie_temp_5', 2000, 1,1)

    for i in range(1,4):
        criticRatedMovie('movie_temp_5', 2000, i, 5)

    assert getFanCritics() == [(3,3),(2,3),(2,2),(1,3),(1,1)]

    deleteStudio(1)

    assert getFanCritics() == [(3, 3), (2, 3), (2, 2), (1, 3)]

    deleteStudio(2)
    deleteStudio(3)

    deleteMovie('movie_temp_1', 2000)
    deleteMovie('movie_temp_3', 2000)
    deleteMovie('movie_temp_4', 2000)
    deleteMovie('movie_temp_5', 2000)

    deleteCritic(1)
    deleteCritic(2)
    deleteCritic(3)

    assert getFanCritics() == []

    print("########## TEST Get Fan Critic END #################\n")

    print("########## TEST Average By Age BEGIN #################")

    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))
    addMovie(Movie('movie_temp_3', 2000, "Drama"))
    addMovie(Movie('movie_temp_4', 2000, "Comedy"))
    addMovie(Movie('movie_temp_5', 2000, "Comedy"))
    addMovie(Movie('movie_temp_6', 2000, "Horror"))


    addActor(Actor(1,'actor_temp_1',10, 1))
    addActor(Actor(2,'actor_temp_2',20, 1))
    addActor(Actor(3,'actor_temp_3',30, 1))
    addActor(Actor(4,'actor_temp_4', 40, 1))
    addActor(Actor(5,'actor_temp_5', 50, 1))

    assert averageAgeByGenre() == []

    actorPlayedInMovie('movie_temp_1', 2000,1, 1, ['R1', 'R2'])
    actorPlayedInMovie('movie_temp_4' ,2000,2, 1, ['R3', 'R4'])


    assert averageAgeByGenre() == [("Drama", 10), ("Comedy", 20)]

    actorPlayedInMovie('movie_temp_5', 2000, 3,1, ['R5'])

    assert averageAgeByGenre() == [("Drama", 10), ("Comedy", 25)]

    actorPlayedInMovie('movie_temp_6', 2000, 3, 1, ['R5'])
    actorPlayedInMovie('movie_temp_6', 2000, 4, 1, ['R5'])
    actorPlayedInMovie('movie_temp_6', 2000, 5, 1, ['R5'])

    assert averageAgeByGenre() == [("Horror", 40),("Drama", 10), ("Comedy", 25)]


    clearTables()

    assert averageAgeByGenre() == []

    print("########## TEST Average By Age END #################\n")

    print("########## TEST Get Exclusive Actors BEGIN #################")
    addMovie(Movie('movie_temp_1', 2000, "Drama"))
    addMovie(Movie('movie_temp_2', 2000, "Drama"))
    addMovie(Movie('movie_temp_3', 2000, "Drama"))
    addMovie(Movie('movie_temp_4', 2000, "Comedy"))
    addMovie(Movie('movie_temp_5', 2000, "Comedy"))


    addActor(Actor(1, 'actor_temp_1', 10, 1))
    addActor(Actor(2, 'actor_temp_2', 20, 1))
    addActor(Actor(3, 'actor_temp_3', 30, 1))
    addActor(Actor(4, 'actor_temp_4', 40, 1))
    addActor(Actor(5, 'actor_temp_5', 50, 1))


    addStudio(Studio(1, 'studio_temp_1'))
    addStudio(Studio(2, 'studio_temp_2'))
    addStudio(Studio(3, 'studio_temp_3'))
    addStudio(Studio(4, 'studio_temp_4'))
    addStudio(Studio(5, 'studio_temp_5'))

    assert getExclusiveActors() == []

    studioProducedMovie(1, 'movie_temp_1', 2000,1, 1)
    studioProducedMovie(2, 'movie_temp_2', 2000,1, 1)
    studioProducedMovie(2, 'movie_temp_3', 2000,1, 1)

    assert getExclusiveActors() == []

    actorPlayedInMovie('movie_temp_1', 2000,1,1, ['R1', 'R2'])

    assert getExclusiveActors() == [(1,1)]

    actorPlayedInMovie('movie_temp_2', 2000, 1, 1, ['R1', 'R2'])
    actorPlayedInMovie('movie_temp_2', 2000, 3, 1, ['R1', 'R2'])

    assert getExclusiveActors() == [(3, 2)]

    actorPlayedInMovie('movie_temp_3', 2000, 4, 1, ['R1'])
    actorPlayedInMovie('movie_temp_1', 2000, 5, 1, ['R1'])

    assert getExclusiveActors() == [(5, 1), (4, 2), (3, 2)]

    print("########## TEST Get Exclusive Actors END #################\n\n")

    print("#################")
    print("#               #")
    print("#   GOOD JOB    #")
    print("#               #")
    print("#################")

    print("\nNOTE: These tests have been made by someone purely human that sometime make mistakes\n")





















