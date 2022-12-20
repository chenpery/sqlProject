class Studio():
    def __init__(self,studio_id=None, studio_name=None):
        self.__id = studio_id
        self.__name = studio_name


    def getStudioName(self):
        return self.__name

    def setStudioName(self, name):
        self.__name = name

    def getStudioID(self):
        return self.__id

    def setStudioID(self, id):
        self.__id = id


    @staticmethod
    def badStudio():
        return Studio()

    def __eq__(self, other):
        return self.__name == other.__name and self.__id == other.__id

    def __str__(self):
        str("StudioName=" + str(self.__name) + ", StudioID=" + str(self.__id))