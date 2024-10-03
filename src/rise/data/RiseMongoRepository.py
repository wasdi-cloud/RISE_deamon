import logging

from src.rise.data.MongoDBClient import MongoDBClient


class RiseMongoRepository:
    # name of the database connected to this repository
    s_sDB_NAME = "rise"

    def __init__(self):
        self.m_sCollectionName = None
        self.m_sEntityClassName = None

    def getCollection(self):
        """
        Retrieves from the database a collection
        :return: the collection if present, None otherwise
        """
        oCollection = None
        try:
            oMongoClient = MongoDBClient()
            oDatabase = oMongoClient.client[RiseMongoRepository.s_sDB_NAME]

            if oDatabase is None:
                logging.warning(f"RiseMongoRepository.getCollection. Database named '{RiseMongoRepository.s_sDB_NAME}' not found in Mongo")
                return None

            oCollection = oDatabase[self.m_sCollectionName]
        except Exception as oEx:
            logging.error(f"RiseMongoRepository.getCollection. Exception retrieving the collection {oEx}")

        return oCollection


    def getEntityById(self, sEntityId):
        """
        Given the id of an entity, retrieves it from the database
        :param sEntityId: the id of the entity (not the Mongo _id, but the RISE internal id of the entity)
        :return: the entity with the required id, None otherwise
        """
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"RiseMongoRepository.findEntityById. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sEntityId})

            if oRetrievedResult is None:
                logging.info(f"RiseMongoRepository.findEntityById. No results retrieved from db")
                return None

            aoRetrievedEntities = []
            for oResEntity in oRetrievedResult:
                oEntityClass = self.getClass(self.m_sEntityClassName)
                aoRetrievedEntities.append(oEntityClass(**oResEntity))

            if len(aoRetrievedEntities) > 0:
                return aoRetrievedEntities[0]
            else:
                return None
        except Exception as oEx:
            logging.error(f"RiseMongoRepository.findEntityById. Exception {oEx}")

        return None

    def listAllEntities(self):
        """
        List all the entities in a collection
        :return: the full list of entities in a collection
        """
        oCollection = self.getCollection()

        if oCollection is None:
            logging.warning(f"RiseMongoRepository.listAllEntities. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
            return None

        try:
            oRetrievedResult = oCollection.find({})

            if oRetrievedResult is None:
                logging.info(f"RiseMongoRepository.listAllEntities. No results retrieved from db")
                return None

            aoRetrievedEntities = []
            for oResEntity in oRetrievedResult:
                oEntityClass = self.getClass(self.m_sEntityClassName)
                aoRetrievedEntities.append(oEntityClass(**oResEntity))

            return aoRetrievedEntities

        except Exception as oEx:
            logging.error(f"RiseMongoRepository.listAllEntities. Exception {oEx}")

        return None

    def getAllEntitiesById(self, asEntityIds):
        """
        Given a list of entities' ids, retrieves from a collection the list of entities matching those ids
        :param asEntityIds: list of entities' ids to retrieve (not the Mongo _id, but the RISE internal id of the entity)
        :return: the list of entities matching the ids passed as parameters
        """
        try:
            if asEntityIds is None or len(asEntityIds) == 0:
                logging.warning("RiseMongoRepository.findAllEntitiesById. No ids specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"RiseMongoRepository.findAllEntitiesById. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": {"$in": asEntityIds}})

            if oRetrievedResult is None:
                logging.info(f"RiseMongoRepository.findAllEntitiesById. No results retrieved from db")
                return None

            aoRetrievedEntities = []
            for oResMap in oRetrievedResult:
                oEntityClass = self.getClass(self.m_sEntityClassName)
                aoRetrievedEntities.append(oEntityClass(**oResMap))

            logging.info(f"RiseMongoRepository.findAllEntitiesById. Retrieved {len(aoRetrievedEntities)} entities")
            return aoRetrievedEntities

        except Exception as oEx:
            logging.error(f"RiseMongoRepository.findAllEntitiesById. Exception {oEx}")

        return None

    def addEntity(self, oEntity):
        """
        Insert an entity in a collection
        :param oEntity: the entity to add to the collection
        :return: True if the entity was successfully added to the collection, False otherwise
        """
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"RiseMongoRepository.add. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return False

            oCollection.insert_one(vars(oEntity))

            return True
        except Exception as oEx:
            logging.error(f"RiseMongoRepository.add. Exception {oEx}")

        return False

    def getClass(self, sClassName):
        asParts = sClassName.split('.')
        oModule = ".".join(asParts[:-1])
        oType = __import__(oModule)
        for sComponent in asParts[1:]:
            oType = getattr(oType, sComponent)
        return oType
