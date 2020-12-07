#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() {
        Attribute attr;
        // Prepare tables recordDescriptor
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        tablesRecordDescriptor.push_back(attr);

        attr.name = "table-name";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) TAB_COL_VC_LEN;
        tablesRecordDescriptor.push_back(attr);

        attr.name = "file-name";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) TAB_COL_VC_LEN;
        tablesRecordDescriptor.push_back(attr);

        // Prepare columns recordDescriptor
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-name";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) TAB_COL_VC_LEN;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-type";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-length";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-position";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-indexed";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnsRecordDescriptor.push_back(attr);

        // initialize internal rbfm and ix
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        this->rbfm = &rbfm;

        IndexManager& ix = IndexManager::instance();
        this->ix = &ix;
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC errCode = rbfm->createFile("Tables");
        if (errCode != 0) return errCode;

        errCode = rbfm->createFile("Columns");
        if (errCode != 0) return errCode;

        errCode = insertTablesRecord(1, "Tables", "Tables");
        if (errCode != 0) return errCode;

        errCode = insertTablesRecord(2,"Columns", "Columns");
        if (errCode != 0) return errCode;

        errCode = insertColumnsRecord(1, tablesRecordDescriptor);
        if (errCode != 0) return errCode;

        errCode = insertColumnsRecord(2, columnsRecordDescriptor);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC errCode = rbfm->destroyFile("Tables");
        if (errCode != 0) return errCode;

        errCode = rbfm->destroyFile("Columns");
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if (tableName == "Tables" || tableName == "Columns") return -1;

        // Try open Tables at input mode. If unsuccessful, doesn't exist
        std::fstream tablesCatalogFile;
        tablesCatalogFile.open("Tables", std::ios::in | std::ios::binary);
        if (tablesCatalogFile.is_open()) tablesCatalogFile.close();
        else return -1;

        // Try open Columns at input mode. If unsuccessful, doesn't exist
        std::fstream columnsCatalogFile;
        columnsCatalogFile.open("Columns", std::ios::in | std::ios::binary);
        if (columnsCatalogFile.is_open()) columnsCatalogFile.close();
        else return -1;

        int tableId = getMaxTableId() + 1;
        RC errCode = insertTablesRecord(tableId, tableName, tableName);
        if (errCode != 0) return errCode;

        errCode = insertColumnsRecord(tableId, attrs);
        if (errCode != 0) return errCode;

        errCode = rbfm->createFile(tableName);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if (tableName == "Tables" || tableName == "Columns") return -1;

        RID TablesRid;
        int tableId;
        RC errCode = getTableId(tableName, TablesRid, tableId);
        if (errCode != 0) return errCode;

        // Delete record from Tables table
        FileHandle tablesFileHandle;
        errCode = rbfm->openFile("Tables", tablesFileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->deleteRecord(tablesFileHandle, tablesRecordDescriptor, TablesRid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(tablesFileHandle);
        if (errCode != 0) return errCode;

        // Delete records from Columns table, and destroy any associated index files
        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("column-indexed");
        attributeNames.emplace_back("column-name");

        RM_ScanIterator rmScanIterator;
        errCode = scan("Columns", "table-id", EQ_OP, &tableId, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        RID ColumnsRid;
        void* columnInfoData = malloc(1+INT_SIZE+TAB_COL_VC_LEN);
        while (rmScanIterator.getNextTuple(ColumnsRid, columnInfoData) != RM_EOF) {
            errCode = rbfm->deleteRecord(rmScanIterator.rbfm_scanIterator.fileHandle, columnsRecordDescriptor, ColumnsRid);
            if (errCode != 0) return errCode;

            int column_indexed;
            memcpy(&column_indexed, (char*) columnInfoData+1, INT_SIZE);

            if (column_indexed) {
                unsigned varCharLen;
                memcpy(&varCharLen, (char*) columnInfoData+1+INT_SIZE, VC_LEN_SIZE);
                char* attrNamePtr = (char*) malloc(varCharLen);
                memcpy(attrNamePtr, (char*) columnInfoData+1+INT_SIZE+VC_LEN_SIZE, varCharLen);
                std::string attributeName = std::string(attrNamePtr, varCharLen);
                free(attrNamePtr);

                // destroy index file
                std::string indexFileName = tableName+"_"+attributeName+".idx";
                errCode = ix->destroyFile(indexFileName);
                if (errCode != 0) return errCode;
            }
        }
        rmScanIterator.close();
        free(columnInfoData);

        errCode = rbfm->destroyFile(tableName);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        // test if index already exist for the attribute
        bool isIndexed;
        RC errCode = getIndexed(tableName, attributeName, isIndexed);
        if (errCode != 0) return errCode;
        if (isIndexed) return -1;  // index already exist

        // update column_indexed for the attribute
        errCode = setIndexed(tableName, attributeName, true);
        if (errCode != 0) return errCode;

        // create index file
        std::string indexFileName = tableName+"_"+attributeName+".idx";
        errCode = ix->createFile(indexFileName);
        if (errCode != 0) return errCode;

        // add all existing entries into index file
        std::vector<std::string> attributeNames;
        attributeNames.emplace_back(attributeName);

        RM_ScanIterator rmScanIterator;
        errCode = scan(tableName, "", NO_OP, nullptr, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        RID rid;
        void* dumData = malloc(PAGE_SIZE);
        while (rmScanIterator.getNextTuple(rid, dumData) != RM_EOF) {
            errCode = insertOrDeleteEntry(tableName, rid, attributeName, indexFileName, true);
            if (errCode != 0) return errCode;
        }
        free(dumData);
        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        // test if index already exist for the attribute
        bool isIndexed;
        RC errCode = getIndexed(tableName, attributeName, isIndexed);
        if (errCode != 0) return errCode;
        if (!isIndexed) return -1;  // index doesn't exist

        // update column_indexed for the attribute
        errCode = setIndexed(tableName, attributeName,false);
        if (errCode != 0) return errCode;

        // destroy index file
        std::string indexFileName = tableName+"_"+attributeName+".idx";
        errCode = ix->destroyFile(indexFileName);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (tableName == "Tables") attrs = tablesRecordDescriptor;
        else if (tableName == "Columns") attrs = columnsRecordDescriptor;
        else {
            RID dumRid;
            int tableId;
            RC errCode = getTableId(tableName, dumRid, tableId);
            if (errCode != 0) return errCode;

            errCode = buildRecordDescriptor(tableId, attrs);
            if (errCode != 0) return errCode;
        }
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void* data, RID &rid) {
        if (tableName == "Tables" || tableName == "Columns") return -1;

        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        // insert record
        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        // insert corresponding entry for all indexed attributes
        bool isIndexed;
        for (Attribute attr : recordDescriptor) {
            errCode = getIndexed(tableName, attr.name, isIndexed);
            if (errCode != 0) return errCode;

            if (isIndexed) {
                std::string indexFileName = tableName+"_"+attr.name+".idx";
                errCode = insertOrDeleteEntry(tableName, rid, attr.name, indexFileName, true);
                if (errCode != 0) return errCode;
            }
        }

        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (tableName == "Tables" || tableName == "Columns") return -1;

        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        // delete corresponding entry for all indexed attributes
        bool isIndexed;
        for (Attribute attr : recordDescriptor) {
            errCode = getIndexed(tableName, attr.name, isIndexed);
            if (errCode != 0) return errCode;

            if (isIndexed) {
                std::string indexFileName = tableName+"_"+attr.name+".idx";
                errCode = insertOrDeleteEntry(tableName, rid, attr.name, indexFileName, false);
                if (errCode != 0) return errCode;
            }
        }

        // delete record
        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void* data, const RID &rid) {
        if (tableName == "Tables" || tableName == "Columns") return -1;

        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        // delete corresponding entry for all indexed attributes
        bool isIndexed;
        for (Attribute attr : recordDescriptor) {
            errCode = getIndexed(tableName, attr.name, isIndexed);
            if (errCode != 0) return errCode;

            if (isIndexed) {
                std::string indexFileName = tableName+"_"+attr.name+".idx";
                errCode = insertOrDeleteEntry(tableName, rid, attr.name, indexFileName, false);
                if (errCode != 0) return errCode;
            }
        }

        // update record
        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        // insert corresponding entry for all indexed attributes
        for (Attribute attr : recordDescriptor) {
            errCode = getIndexed(tableName, attr.name, isIndexed);
            if (errCode != 0) return errCode;

            if (isIndexed) {
                std::string indexFileName = tableName+"_"+attr.name+".idx";
                errCode = insertOrDeleteEntry(tableName, rid, attr.name, indexFileName, true);
                if (errCode != 0) return errCode;
            }
        }

        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void* data) {
        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void* data, std::ostream &out) {
        return rbfm->printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void* data) {
        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void* value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        RC errCode = rbfm->openFile(tableName, rm_ScanIterator.rbfm_scanIterator.fileHandle);
        if (errCode != 0) return errCode;

        std::vector<Attribute> recordDescriptor;
        errCode = getAttributes(tableName,recordDescriptor);
        if (errCode != 0) return errCode;

        errCode = rbfm->scan(recordDescriptor, conditionAttribute, compOp, value,
                             attributeNames, rm_ScanIterator.rbfm_scanIterator);
        if (errCode != 0) return errCode;

        return 0;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName,
                                  const void *lowKey, const void *highKey,
                                  bool lowKeyInclusive, bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        std::string indexFileName = tableName+"_"+attributeName+".idx";
        RC errCode = ix->openFile(indexFileName, *rm_IndexScanIterator.ix_scanIterator.ixFileHandle);
        if (errCode != 0) return errCode;

        Attribute attribute;
        errCode = buildAttrDescriptor(tableName, attributeName, attribute);
        if (errCode != 0) return errCode;

        errCode = ix->scan(*rm_IndexScanIterator.ix_scanIterator.ixFileHandle, attribute, lowKey, highKey,
                           lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scanIterator);
        if (errCode != 0) return errCode;

        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    RC RelationManager::insertTablesRecord(int table_id, const std::string &table_name, const std::string &file_name) {
        FileHandle fileHandle;
        RC errCode = rbfm->openFile("Tables", fileHandle);
        if (errCode != 0) return errCode;

        // Generate record for Tables table
        char* recordBuffer = (char*) malloc(TAB_COL_NULL_SIZE+INT_SIZE+2*VC_LEN_SIZE+2*TAB_COL_VC_LEN);
        unsigned recordBufferPtr = 0;

        char nullIndicator = 0;  // 00000000
        memcpy((char*) recordBuffer+recordBufferPtr, &nullIndicator, TAB_COL_NULL_SIZE);
        recordBufferPtr += TAB_COL_NULL_SIZE;

        memcpy((char*) recordBuffer+recordBufferPtr, &table_id, INT_SIZE);
        recordBufferPtr += INT_SIZE;

        unsigned varCharLen = table_name.size();
        memcpy((char*) recordBuffer+recordBufferPtr, &varCharLen, VC_LEN_SIZE);
        recordBufferPtr += VC_LEN_SIZE;
        memcpy((char*) recordBuffer+recordBufferPtr, table_name.c_str(), varCharLen);
        recordBufferPtr += varCharLen;

        varCharLen = file_name.size();
        memcpy((char*) recordBuffer+recordBufferPtr, &varCharLen, VC_LEN_SIZE);
        recordBufferPtr += VC_LEN_SIZE;
        memcpy((char*) recordBuffer+recordBufferPtr, file_name.c_str(), varCharLen);

        // Insert record to Tables table
        RID dumRid;
        errCode = rbfm->insertRecord(fileHandle, tablesRecordDescriptor, recordBuffer, dumRid);
        if (errCode != 0) return errCode;

        free(recordBuffer);

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::insertColumnsRecord(int table_id, const std::vector<Attribute>& recordDescriptor) {
        FileHandle fileHandle;
        RC errCode = rbfm->openFile("Columns", fileHandle);
        if (errCode != 0) return errCode;

        char* recordBuffer = (char*) malloc(
                TAB_COL_NULL_SIZE+3*INT_SIZE+VC_LEN_SIZE+TAB_COL_VC_LEN+ATTR_TYPE_SIZE+ATTR_LEN_SIZE);
        int column_position = 1;
        int column_indexed = 0;
        for (Attribute attr : recordDescriptor) {
            // Generate record for Columns table
            unsigned recordBufferPtr = 0;

            char nullIndicator = 0;  // 00000000
            memcpy((char*) recordBuffer+recordBufferPtr, &nullIndicator, TAB_COL_NULL_SIZE);
            recordBufferPtr += TAB_COL_NULL_SIZE;

            memcpy((char*) recordBuffer+recordBufferPtr, &table_id, INT_SIZE);
            recordBufferPtr += INT_SIZE;

            unsigned varCharLen = attr.name.size();
            memcpy((char*) recordBuffer+recordBufferPtr, &varCharLen, VC_LEN_SIZE);
            recordBufferPtr += VC_LEN_SIZE;
            memcpy((char*) recordBuffer+recordBufferPtr, attr.name.c_str(), varCharLen);
            recordBufferPtr += varCharLen;

            memcpy((char*) recordBuffer+recordBufferPtr, &attr.type, ATTR_TYPE_SIZE);
            recordBufferPtr += ATTR_TYPE_SIZE;

            memcpy((char*) recordBuffer+recordBufferPtr, &attr.length, ATTR_LEN_SIZE);
            recordBufferPtr += ATTR_LEN_SIZE;

            memcpy((char*) recordBuffer+recordBufferPtr, &column_position, INT_SIZE);
            recordBufferPtr += INT_SIZE;

            memcpy((char*) recordBuffer+recordBufferPtr, &column_indexed, INT_SIZE);
            column_position++;

            // Insert record to Columns table
            RID dumRid;
            errCode = rbfm->insertRecord(fileHandle, columnsRecordDescriptor, recordBuffer, dumRid);
            if (errCode != 0) return errCode;
        }

        free(recordBuffer);

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::getTableId(const std::string &tableName, RID &rid, int& tableId) {
        unsigned varCharLen = tableName.size();
        void* value = malloc(VC_LEN_SIZE + varCharLen);
        memcpy(value, &varCharLen, VC_LEN_SIZE);
        memcpy((char*) value + VC_LEN_SIZE, tableName.c_str(), varCharLen);

        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("table-id");

        RM_ScanIterator rmScanIterator;
        RC errCode = scan("Tables", "table-name", EQ_OP, value, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        void* data = malloc(1+INT_SIZE);
        if (rmScanIterator.getNextTuple(rid, data) != RM_EOF) {
            memcpy(&tableId, (char*) data+1, INT_SIZE);
            rmScanIterator.close();
            free(value);
            free(data);
            return 0;
        }
        // Didn't find table
        else {
            rmScanIterator.close();
            free(value);
            free(data);
            return -1;
        }
    }

    RC RelationManager::buildRecordDescriptor(int tableId, std::vector<Attribute> &attrs) {
        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("column-name");
        attributeNames.emplace_back("column-type");
        attributeNames.emplace_back("column-length");

        RM_ScanIterator rmScanIterator;
        RC errCode = scan("Columns", "table-id", EQ_OP, &tableId, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        RID dumRid;
        void* data = malloc(1+VC_LEN_SIZE+TAB_COL_VC_LEN+ATTR_TYPE_SIZE+ATTR_LEN_SIZE);
        while (rmScanIterator.getNextTuple(dumRid, data) != RM_EOF) {
            unsigned dataPtr = 1;
            Attribute attr;

            unsigned varCharLen;
            memcpy(&varCharLen, (char*) data+dataPtr, VC_LEN_SIZE);
            dataPtr += VC_LEN_SIZE;
            char* columnName = (char*) malloc(varCharLen);
            memcpy(columnName, (char*) data+dataPtr, varCharLen);
            dataPtr += varCharLen;
            attr.name = std::string(columnName, varCharLen);
            free(columnName);

            AttrType columnType;
            memcpy(&columnType, (char*) data+dataPtr, ATTR_TYPE_SIZE);
            dataPtr += ATTR_TYPE_SIZE;
            attr.type = columnType;

            AttrLength columnLength;
            memcpy(&columnLength, (char*) data+dataPtr, ATTR_LEN_SIZE);
            attr.length = columnLength;

            attrs.push_back(attr);
        }
        rmScanIterator.close();
        free(data);
        return 0;
    }

    int RelationManager::getMaxTableId() {
        // Try open Tables at input mode. If unsuccessful, doesn't exist
        std::fstream tablesCatalogFile;
        tablesCatalogFile.open("Tables", std::ios::in | std::ios::binary);
        if (tablesCatalogFile.is_open()) tablesCatalogFile.close();
        else return 0;

        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("table-id");

        RM_ScanIterator rmScanIterator;
        RC errCode = scan("Tables", "", NO_OP, nullptr, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        RID dumRid;
        void* data = malloc(1+INT_SIZE);
        int maxTableId = 0;
        int currTableId = 0;
        while (rmScanIterator.getNextTuple(dumRid, data) != RM_EOF) {
            memcpy(&currTableId, (char*) data+1, INT_SIZE);
            if (currTableId > maxTableId) {
                maxTableId = currTableId;
            }
        }
        free(data);
        rmScanIterator.close();
        return maxTableId;
    }

    RC RelationManager::getColumnRid(const std::string &tableName, const std::string &attributeName, RID& ColumnsRid) {
        RID dumRid;
        int tableId;
        RC errCode = getTableId(tableName, dumRid, tableId);
        if (errCode != 0) return errCode;

        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("column-name");

        RM_ScanIterator rmScanIterator;
        errCode = scan("Columns", "table-id", EQ_OP, &tableId, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        void* columnNameData = malloc(TAB_COL_VC_LEN);
        bool attributeFound = false;
        while (rmScanIterator.getNextTuple(ColumnsRid, columnNameData) != RM_EOF) {
            unsigned varCharLen;
            memcpy(&varCharLen, (char*) columnNameData+1, VC_LEN_SIZE);
            char* dataVarChar = (char*) malloc(varCharLen);
            memcpy(dataVarChar, (char*) columnNameData+1+VC_LEN_SIZE, varCharLen);

            if (std::string(dataVarChar, varCharLen) == attributeName) {
                attributeFound = true;
                free(dataVarChar);
                break;
            }
            free(dataVarChar);
        }
        rmScanIterator.close();
        free(columnNameData);

        if (attributeFound) return 0;
        else return -1;
    }

    RC RelationManager::getIndexed(const std::string &tableName, const std::string &attributeName, bool& isIndexed) {
        // find the RID in "Columns" table of the table-attribute pair
        RID ColumnsRid;
        RC errCode = getColumnRid(tableName, attributeName, ColumnsRid);
        if (errCode != 0) return errCode;

        FileHandle columnsFileHandle;
        errCode = rbfm->openFile("Columns", columnsFileHandle);
        if (errCode != 0) return errCode;

        void* isIndexedData = malloc(1+INT_SIZE);
        errCode = rbfm->readAttribute(columnsFileHandle, columnsRecordDescriptor, ColumnsRid, "column-indexed", isIndexedData);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(columnsFileHandle);
        if (errCode != 0) return errCode;

        int column_indexed;
        memcpy(&column_indexed, (char*) isIndexedData+1, INT_SIZE);
        free(isIndexedData);

        if (column_indexed) isIndexed = true;
        else isIndexed = false;
        return 0;
    }

    RC RelationManager::setIndexed(const std::string &tableName, const std::string &attributeName, bool isIndexed) {
        // find the RID in "Columns" table of the table-attribute pair
        RID ColumnsRid;
        RC errCode = getColumnRid(tableName, attributeName, ColumnsRid);
        if (errCode != 0) return errCode;

        char* columnsRecord = (char*) malloc(TAB_COL_NULL_SIZE+3*INT_SIZE+VC_LEN_SIZE+attributeName.size()+ATTR_TYPE_SIZE+ATTR_LEN_SIZE);
        FileHandle columnsFileHandle;
        errCode = rbfm->openFile("Columns", columnsFileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->readRecord(columnsFileHandle, columnsRecordDescriptor, ColumnsRid, columnsRecord);
        if (errCode != 0) return errCode;

        int column_indexed;
        if (isIndexed) column_indexed = 1;
        else column_indexed = 0;
        memcpy(columnsRecord+TAB_COL_NULL_SIZE+2*INT_SIZE+VC_LEN_SIZE+attributeName.size()+ATTR_TYPE_SIZE+ATTR_LEN_SIZE, &column_indexed, INT_SIZE);

        errCode = rbfm->updateRecord(columnsFileHandle, columnsRecordDescriptor, columnsRecord, ColumnsRid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(columnsFileHandle);
        if (errCode != 0) return errCode;
        free(columnsRecord);
        return 0;
    }

    RC RelationManager::buildAttrDescriptor(const std::string &tableName, const std::string &attributeName, Attribute& attribute) {
        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        for (Attribute attr : recordDescriptor) {
            if (attr.name == attributeName) {
                attribute = attr;
                break;
            }
        }
        return 0;
    }

    RC RelationManager::insertOrDeleteEntry(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                            const std::string &indexFileName, bool insert) {
        void* entryData = malloc(PAGE_SIZE);
        RC errCode = readAttribute(tableName, rid, attributeName, entryData);
        if (errCode != 0) return errCode;

        Attribute attribute;
        errCode = buildAttrDescriptor(tableName, attributeName, attribute);
        if (errCode != 0) return errCode;

        unsigned short entryLength;
        if (attribute.type == TypeVarChar) {
            unsigned varCharLen;
            memcpy(&varCharLen, (char*) entryData+1, VC_LEN_SIZE);
            entryLength = VC_LEN_SIZE + varCharLen;
        }
        else {
            entryLength = INT_OR_FLT_SIZE;
        }

        void* entry = malloc(entryLength);
        memcpy(entry, (char*) entryData+1, entryLength);
        free(entryData);

        IXFileHandle ixFileHandle;
        errCode = ixFileHandle.fileHandle.openFile(indexFileName);
        if (errCode != 0) return errCode;

        if (insert) errCode = ix->insertEntry(ixFileHandle, attribute, entry, rid);
        else errCode = ix->deleteEntry(ixFileHandle, attribute, entry, rid);
        if (errCode != 0) return errCode;
        free(entry);

        errCode = ixFileHandle.fileHandle.closeFile();
        if (errCode != 0) return errCode;

        return 0;
    }

    /***********************************************/
    /*******   functions of RM_ScanIterator  *******/
    /***********************************************/
    RC RM_ScanIterator::getNextTuple(RID &rid, void* data) {
        RC errCode = rbfm_scanIterator.getNextRecord(rid, data);
        if (errCode == RBFM_EOF) return RM_EOF;
        else return errCode;
    }

    RC RM_ScanIterator::close() {
        RC errCode = rbfm_scanIterator.close();
        if (errCode != 0) return errCode;

        errCode = rbfm_scanIterator.fileHandle.closeFile();
        if (errCode != 0) return errCode;
        return 0;
    }

    /*************************************************/
    /*****  functions of RM_IndexScanIterator  *******/
    /*************************************************/
    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        RC errCode = ix_scanIterator.getNextEntry(rid, key);
        if (errCode == IX_EOF) return RM_EOF;
        else return errCode;
    }

    RC RM_IndexScanIterator::close() {
        RC errCode = ix_scanIterator.close();
        if (errCode != 0) return errCode;

        errCode = ix_scanIterator.ixFileHandle->fileHandle.closeFile();
        if (errCode != 0) return errCode;
        return 0;
    }

} // namespace PeterDB