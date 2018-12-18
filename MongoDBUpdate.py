########################################################################################################################
#   File:
#
#   Description:
#
#   Input:
#
#   Author:
#
#   Contributors:
#
#   Change History:
#
#   Version         By                      Desc
#   -------         --                      ----
########################################################################################################################

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



########################################################################################################################
#   Class:
#   Input:
#   Output:
#   Desc:
########################################################################################################################

class MongoDBUpdate():

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

    def __init__(self):
        self.host = ""
        self.port = ""

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

    def usage(self):
        print("Usage")

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

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

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

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

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

    def doMongoAddForOneDocument(self, action):
        print("Inside doMOngoAddForOneDocument")
        self.action = action
        connection = self._createConnection()
        print(action)
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

        print("Adding complete!!")

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

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

        print("Adding complete!!")

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

    def doMongoDeleteOneDocument(self, action):
        print("Inside doMongoDeleteOneDocument")
        self.action = action
        connection = self._createConnection()
        jsonFile = self._checkIfJsonFileExists(self.action)
        print("Jsonfile is ", jsonFile)
        with open(jsonFile) as DelFile:
            DelData = json.load(DelFile)

        database = DelData['DB']
        collection = DelData['Collection']
        dbInstance = connection[database][collection]
        # print((addData['FieldsToAdd']))
        # print(addData['FieldsToAdd']['Company'])
        new_dict = DelData['FieldsToDelete']
        condition_dict = DelData['ConditionField']
        for key, value in condition_dict.items():
            conditionKey = key
            conditionValue = value

        for key, value in new_dict.items():
            print("key is ", key, " and value is ", value)
            dbInstance.update_many({conditionKey: conditionValue}, {"$unset": {key: value}}, True)

        print("Delete complete!!")

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################
    def doMongoDeleteAllDocuments(self,action):
        print("Inside doMongoDeleteAllDocuments")
        self.action = action
        connection = self._createConnection()
        jsonFile = self._checkIfJsonFileExists(self.action)
        print("Jsonfile is ", jsonFile)
        with open(jsonFile) as DelFile:
            DelData = json.load(DelFile)
        # print(addData['DB'])
        # print(addData['Collection'])
        # TODO
        # Make the entries generic. They should be read from json file.
        database = DelData['DB']
        collection = DelData['Collection']
        dbInstance = connection[database][collection]
        # collectionDocuments = dbInstance.find({})
        # TODO
        # Need to check if a condition field is required to update all records.
        # print(collectionDocuments.count())
        print((DelData['FieldsToDelete']))
        new_dict = DelData['FieldsToDelete']
        for key, value in new_dict.items():
            print("key is ", key, " and value is ", value)
            dbInstance.update_many({}, {"$unset": {key: value}}, True)

        print("Delete Complete!!")
    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################
    def doMongoUpdateOneField(self, action):
        print("Inside doMongoUpdateOneField")
        self.action = action
        connection = self._createConnection()
        jsonFile = self._checkIfJsonFileExists(self.action)
        print("Jsonfile is ", jsonFile)
        with open(jsonFile) as updFile:
            UpdData = json.load(updFile)

        database = UpdData['DB']
        collection = UpdData['Collection']
        dbInstance = connection[database][collection]
        # print((addData['FieldsToAdd']))
        # print(addData['FieldsToAdd']['Company'])
        new_dict = UpdData['FieldsToUpdate']
        condition_dict = UpdData['ConditionField']
        for key, value in condition_dict.items():
            conditionKey = key
            conditionValue = value

        for field, new_field in new_dict.items():
            print("key is ", field, " and value is ", new_field)
            dbInstance.update_many({conditionKey: conditionValue}, {"$rename": {field: new_field}}, True)

        print("Update complete!!")


    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

    def doMongoUpdateAllFields(self, action):
        print("Inside doMongoUpdateAllFields")
        self.action = action
        connection = self._createConnection()
        jsonFile = self._checkIfJsonFileExists(self.action)
        print("Jsonfile is ", jsonFile)
        with open(jsonFile) as updFile:
            UpdData = json.load(updFile)

        database = UpdData['DB']
        collection = UpdData['Collection']
        dbInstance = connection[database][collection]
        # print((addData['FieldsToAdd']))
        # print(addData['FieldsToAdd']['Company'])
        new_dict = UpdData['FieldsToUpdate']

        for field, new_field in new_dict.items():
            print("key is ", field, " and value is ", new_field)
            dbInstance.update_many({}, {"$rename": {field: new_field}}, True)
        print("Update complete!!")

    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################
    def Func(self,action):
        Func1 = {
            'AddAll': self.doMongoAddForAllDocuments,
            'AddOne': self.doMongoAddForOneDocument,
            'DeleteAll': self.doMongoDeleteAllDocuments,
            'DeleteOne': self.doMongoDeleteOneDocument,
            'UpdateOne': self.doMongoUpdateOneField,
            'UpdateAll': self.doMongoUpdateAllFields
        }
        print(Func1[action])
        return Func1[action]
    ####################################################################################################################
    #   Function:
    #   Input:
    #   Output:
    #   Desc:
    ####################################################################################################################

    def doCallMethod(self, action):
        print("in doCallMethod action is ", action)
        return self.Func(action)(action)
        # if action == ADD_ONE:
        #     MongoInstance.doMongoAddForOneDocument(action)
        # elif action == ADD_ALL:
        #     MongoInstance.doMongoAddForAllDocuments(action)
        # else:
        #     pass

########################################################################################################################
#   Function:
#   Input:
#   Output:
#   Desc:
########################################################################################################################

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

########################################################################################################################
#   Function:
#   Input:
#   Output:
#   Desc:
########################################################################################################################

if __name__=='__main__':
    main()