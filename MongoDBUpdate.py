import pymongo, json, configparser, sys, os
dbConfigFile = 'conf/database.conf'
JSON_PATH = 'JSON/'
JSON_EXTN = '.json'
ADD_ALL = 'AddAll'
ADD_ONE = 'AddOne'
DELETE_ALL = 'DeleteAll'
DELETE_ONE = 'DeleteOne'
UPDATE_FIELD = 'UpdateField'
UPDATE_VALUE = 'UpdateValue'

class MongoDBUpdate():
    def __init__(self):
        self.host = ""
        self.port = ""

    def usage(self):
        print("Usage")

    def _createConnection(self):
        config = configparser.ConfigParser()
        config.read(dbConfigFile)
        self.host = config['database']['host']
        self.port = config['database']['port']
        try:
            client = pymongo.MongoClient(self.host, int(self.port))
            print("Connected to MongoDB")
            return client
        except:
            raise("Could not connect to MongoDB. Exiting!!")
            sys.exit(1)

        #db = client.database_name

    def _checkIfJsonFileExists(self, action):
        fileNamePattern = action + JSON_EXTN
        # TODO
        # Do pattern matching for lower case of json files

        for jsonFile in os.listdir(JSON_PATH):
            if jsonFile.endswith(fileNamePattern):
                return JSON_PATH+jsonFile
            else:
                print("The required JSON file is not present in the JSON folder!!")
                sys.exit(2)


    def doMongoAddForOneDocument(self, action):
        print("Inside doMOngoAddForOneDocument")
        self.action = action
        connection = self._createConnection()
        jsonFile = self._checkIfJsonFileExists(self.action)
        print("Jsonfile is ", jsonFile)
        with open(jsonFile) as addFile:
            addData = json.load(addFile)

        database = addData['DB']
        collection = addData['Collection']
        dbInstance = connection[database][collection]
        # print((addData['FieldsToAdd']))
        # print(addData['FieldsToAdd']['Company'])
        new_dict = addData['FieldsToAdd']
        condition_dict = addData['ConditionField']
        for key,value in condition_dict.items():
            conditionKey = key
            conditionValue = value

        for key, value in new_dict.items():
            print("key is ", key, " and value is ", value)
            dbInstance.update_many({conditionKey:conditionValue}, {"$set": {key: value}}, True)

        print("Update complete!!")

    def doMongoAddForAllDocuments(self, action):
        print("Inside doMOngoAddForAllDocuments")
        self.action = action
        connection = self._createConnection()
        jsonFile = self._checkIfJsonFileExists(self.action)
        print("Jsonfile is ", jsonFile)
        with open (jsonFile) as addFile:
            addData = json.load(addFile)
        # print(addData['DB'])
        # print(addData['Collection'])
        # TODO
        # Make the entries generic. They should be read from json file.
        database = addData['DB']
        collection = addData['Collection']
        dbInstance = connection[database][collection]
        #collectionDocuments = dbInstance.find({})
        # TODO
        # Need to check if a condition field is required to update all records.
        #print(collectionDocuments.count())
        print((addData['FieldsToAdd']))
        print(addData['FieldsToAdd']['Company'])
        new_dict = addData['FieldsToAdd']
        for key, value in new_dict.items():
            print("key is ", key, " and value is ", value)
            dbInstance.update_many({},{"$set":{key:value}}, True)

        print("Update complete!!")

    def doMongoDelete(self, action):
        # TODO
        pass

    def doMongoUpdateField(self, action):
        # TODO
        pass

    def doMongoUpdateValue(self, action):
        # TODO
        pass

    def doCallMethod(self, action):
        print("in doCallMethod action is ", action)
        MongoInstance = MongoDBUpdate()
        # return{
        #     ADD_ALL:MongoInstance.doMongoAddForAllDocuments(action),
        #     ADD_ONE:MongoInstance.doMongoAddForOneDocument(action),
        #     DELETE_ALL:MongoInstance.doMongoDelete(action),
        #     DELETE_ONE:MongoInstance.doMongoDelete(action),
        #     UPDATE_FIELD:MongoInstance.doMongoUpdateField(action),
        #     UPDATE_VALUE:MongoInstance.doMongoUpdateValue(action)
        # }
        if action == ADD_ONE:
            MongoInstance.doMongoAddForOneDocument(action)
        elif action == ADD_ALL:
            MongoInstance.doMongoAddForAllDocuments(action)
        else:
            pass

def main():
    MongoInstance = MongoDBUpdate()
    print("Inside main")
    print("len of sys.argv is ", len(sys.argv))
    print("sys.argv is ", sys.argv)
    if len(sys.argv) < 2:
        MongoInstance.usage()
        sys.exit(1)

    action = sys.argv[1]
    print("Action is ", action)
    MongoInstance.doCallMethod(action)


if __name__=='__main__':
    main()