class Critic:
    def __init__(self,critic_id=None, critic_name=None):
        self.__id = critic_id
        self.__name = critic_name


    def getName(self):
        return self.__name

    def setName(self, name):
        self.__name = name

    def getCriticID(self):
        return self.__id

    def setCriticID(self, id):
        self.__id = id

    @staticmethod
    def badCritic():
        return Critic()

    def __eq__(self, other):
        return self.__name == other.__name and self.__id == other.__id

    def __str__(self):
        str("CriticName=" + str(self.__name) + ", CriticID=" + str(self.__id))

