class Actor:
    def __init__(self, actor_id=None, actor_name=None, age=None, height=None):
        self.__id = actor_id
        self.__name = actor_name
        self.__age = age
        self.__height = height

    def getActorID(self):
        return self.__id

    def setActorID(self, id):
        self.__id = id

    def getActorName(self):
        return self.__name

    def setActorName(self, name):
        self.__name = name

    def getAge(self):
        return self.__age

    def setAge(self, age):
        self.__age = age

    def getHeight(self):
        return self.__height

    def setHeight(self, height):
        self.__height = height

    @staticmethod
    def badActor():
        return Actor()

    def __eq__(self, other):
        return self.__id == other.__id and self.__name == other.__name and self.__age == other.__age and\
               self.__height == other.__height

    def __str__(self):
        return str("ActorID=" + str(self.__id) + ", ActorName=" + str(self.__name) + ", Age=" + str(self.__age) + ", Height=" +
              str(self.__height))

