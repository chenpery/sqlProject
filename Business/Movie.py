class Movie:
    def __init__(self, movie_name=None, year=None, genre=None):
        self.__name = movie_name
        self.__year = year
        self.__genre = genre

    def getMovieName(self):
        return self.__name

    def setMovieName(self, name):
        self.__name = name

    def getYear(self):
        return self.__year

    def setYear(self, year):
        self.__year = year

    def getGenre(self):
        return self.__genre

    def setGenre(self, genre):
        self.__genre = genre

    @staticmethod
    def badMovie():
        return Movie()

    def is_bad(self):
        return self.__name is None and self.__year is None and self.__genre is None

    def __eq__(self, other):
        return self.__name == other.__name and self.__year == other.__year and self.__genre == other.__genre

    def __str__(self):
        str("MovieName=" + str(self.__name) + ", Year=" + str(self.__year) + ", Genre=" + str(self.__genre))
