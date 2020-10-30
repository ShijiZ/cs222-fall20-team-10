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
        attr.length = (AttrLength) 4;
        tablesRecordDescriptor.push_back(attr);

        attr.name = "table-name";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        tablesRecordDescriptor.push_back(attr);

        attr.name = "file-name";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        tablesRecordDescriptor.push_back(attr);

        // Prepare columns recordDescriptor
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-name";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-type";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-length";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        columnsRecordDescriptor.push_back(attr);

        attr.name = "column-position";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        columnsRecordDescriptor.push_back(attr);

        numTables = getNumTables();
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        numTables++;
        RC errCode = rbfm->createFile("Tables.clg");
        if (errCode != 0) return errCode;

        numTables++;
        errCode = rbfm->createFile("Columns.clg");
        if (errCode != 0) return errCode;

        errCode = insertTablesRecord(1, "Tables", "Tables.clg");
        if (errCode != 0) return errCode;

        errCode = insertTablesRecord(2,"Columns", "Columns.clg");
        if (errCode != 0) return errCode;

        errCode = insertColumnsRecord(1, tablesRecordDescriptor);
        if (errCode != 0) return errCode;

        errCode = insertColumnsRecord(2, columnsRecordDescriptor);
        if (errCode != 0) return errCode;

        /*
        void* recordBuffer = malloc(105);

        generateTablesRecord(1, "Tables", "Tables.clg", recordBuffer);
        insertTablesOrColumnsTuple("Tables", recordBuffer);

        generateTablesRecord(2, "Columns", "Columns.clg", recordBuffer);
        insertTablesOrColumnsTuple("Tables", recordBuffer);

        generateColumnsRecord(1, "table-id", TypeInt, 4, 1, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(1, "table-name", TypeVarChar, 50, 2, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(1, "file-name", TypeVarChar, 50, 3, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(2, "table-id", TypeInt, 4, 1, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(2, "column-name", TypeVarChar, 50, 2, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(2, "column-type", TypeInt, 4, 3, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(2, "column-length", TypeInt, 4, 4, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);

        generateColumnsRecord(2, "column-position", TypeInt, 4, 5, recordBuffer);
        insertTablesOrColumnsTuple("Columns", recordBuffer);
         */

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC errCode = rbfm->destroyFile("Tables.clg");
        if (errCode != 0) return errCode;

        errCode = rbfm->destroyFile("Columns.clg");
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        numTables++;
        RC errCode = insertTablesRecord(numTables, tableName, tableName + ".tbl");
        if (errCode != 0) return errCode;

        errCode = insertColumnsRecord(numTables, attrs);
        if (errCode != 0) return errCode;

        errCode = rbfm->createFile(tableName + ".tbl");
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        char* data = (char*) malloc(5);
        RID TablesRid;
        RC errCode = getTableId(tableName, TablesRid, data);
        if (errCode != 0) return errCode;

        int tableId;
        memcpy(&tableId, (char*) data+1, sizeof(int));

        // Delete record from Tables table
        errCode = rbfm->openFile("Tables.clg", fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->deleteRecord(fileHandle, tablesRecordDescriptor, TablesRid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        // Delete records form Columns table
        std::string conditionAttribute = "table-id";

        CompOp compOp = EQ_OP;

        std::vector<std::string> attributeNames;
        attributeNames.push_back("table-id");

        int* value = &tableId;

        RM_ScanIterator rmScanIterator;
        scan("Columns.clg", conditionAttribute, compOp, value, attributeNames, rmScanIterator);

        errCode = rbfm->openFile("Columns.clg", fileHandle);
        if (errCode != 0) return errCode;

        RID ColumnsRid;
        while (rmScanIterator.getNextTuple(ColumnsRid, data) != RBFM_EOF) {
            errCode = rbfm->deleteRecord(fileHandle, tablesRecordDescriptor, ColumnsRid);
            if (errCode != 0) return errCode;
        }
        rmScanIterator.close();
        free(data);

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->destroyFile(tableName + ".tbl");
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        char* data = (char*) malloc(5);
        RID dumRid;
        RC errCode = getTableId(tableName, dumRid, data);
        if (errCode != 0) return errCode;

        int tableId;
        memcpy(&tableId, (char*) data+1, sizeof(int));

        return buildRecordDescriptor(tableId, attrs);
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RC errCode = rbfm->openFile(tableName + ".tbl", fileHandle);
        if (errCode != 0) return errCode;

        std::vector<Attribute> recordDescriptor;
        errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        errCode = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RC errCode = rbfm->openFile(tableName + ".tbl", fileHandle);
        if (errCode != 0) return errCode;

        std::vector<Attribute> recordDescriptor;
        errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        errCode = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RC errCode = rbfm->openFile(tableName + ".tbl", fileHandle);
        if (errCode != 0) return errCode;

        std::vector<Attribute> recordDescriptor;
        errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        errCode = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RC errCode = rbfm->openFile(tableName + ".tbl", fileHandle);
        if (errCode != 0) return errCode;

        std::vector<Attribute> recordDescriptor;
        errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

        errCode = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return rbfm->printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        RC errCode = rbfm->openFile(tableName + ".tbl", fileHandle);
        if (errCode != 0) return errCode;

        std::vector<Attribute> recordDescriptor;
        errCode = getAttributes(tableName, recordDescriptor);
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
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    /*
    void RelationManager::generateTablesRecord(int table_id, const std::string &table_name,
                              const std::string &file_name, void *recordBuffer) {

    }

    void RelationManager::generateColumnsRecord(int table_id, const std::string &column_name, AttrType column_type,
                               int column_length, int column_position, void *recordBuffer) {

    }

    RC RelationManager::insertTablesOrColumnsTuple(const std::string &tableName, const void *data) {
        FileHandle fileHandle;
        RC errCode = rbfm->openFile(tableName + ".clg", fileHandle);
        if (errCode != 0) return errCode;
        RID dumRid;

        if (tableName == "Tables") {
            errCode = rbfm->insertRecord(fileHandle, tablesRecordDescriptor, data, dumRid);
        }
        else {
            errCode = rbfm->insertRecord(fileHandle, columnsRecordDescriptor, data, dumRid);
        }

        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }
     */

    RC RelationManager::insertTablesRecord(int table_id, const std::string &table_name, const std::string &file_name) {
        RC errCode = rbfm->openFile("Tables.clg", fileHandle);
        if (errCode != 0) return errCode;

        // Generate record for Tables table
        char *recordBuffer = (char*) malloc(113);
        int recordBufferPtr = 0;

        char nullIndicator = 0;  // 00000000
        memcpy((char*) recordBuffer+recordBufferPtr, &nullIndicator, 1);
        recordBufferPtr += 1;

        memcpy((char*) recordBuffer+recordBufferPtr, &table_id, sizeof(int));
        recordBufferPtr += sizeof(int);

        unsigned varCharLen = table_name.size();
        memcpy((char*) recordBuffer+recordBufferPtr, &varCharLen, sizeof(unsigned));
        recordBufferPtr += sizeof(unsigned);
        memcpy((char*) recordBuffer+recordBufferPtr, table_name.c_str(), varCharLen);
        recordBufferPtr += varCharLen;

        varCharLen = file_name.size();
        memcpy((char*) recordBuffer+recordBufferPtr, &varCharLen, sizeof(unsigned));
        recordBufferPtr += sizeof(unsigned);
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

    RC RelationManager::insertColumnsRecord(int table_id, std::vector<Attribute> recordDescriptor) {
        RC errCode = rbfm->openFile("Columns.clg", fileHandle);
        if (errCode != 0) return errCode;

        char *recordBuffer = (char*) malloc(71);
        int column_position = 1;
        for (Attribute attr : recordDescriptor) {
            // Generate record for Columns table
            int recordBufferPtr = 0;

            char nullIndicator = 0;  // 00000000
            memcpy((char*) recordBuffer+recordBufferPtr, &nullIndicator, 1);
            recordBufferPtr += 1;

            memcpy((char*) recordBuffer+recordBufferPtr, &table_id, sizeof(int));
            recordBufferPtr += sizeof(int);

            unsigned varCharLen = attr.name.size();
            memcpy((char*) recordBuffer+recordBufferPtr, &varCharLen, sizeof(unsigned));
            recordBuffer += sizeof(unsigned);
            memcpy((char*) recordBuffer+recordBufferPtr, attr.name.c_str(), varCharLen);
            recordBuffer += varCharLen;

            memcpy((char*) recordBuffer+recordBufferPtr, &attr.type, sizeof(AttrType));
            recordBuffer += sizeof(AttrType);

            memcpy((char*) recordBuffer+recordBufferPtr, &attr.length, sizeof(AttrLength));
            recordBuffer += sizeof(AttrLength);

            memcpy((char*) recordBuffer+recordBufferPtr, &column_position, sizeof(int));
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

    unsigned RelationManager::getNumTables() {
        // Try open Tables.clg at input mode. If unsuccessful, doesn't exist
        std::fstream tablesCatalogFile;
        tablesCatalogFile.open("Tables.clg", std::ios::in | std::ios::binary);
        if (tablesCatalogFile.is_open()) {
            tablesCatalogFile.close();
        }
        else {
            return 0;
        }

        std::vector<std::string> attributeNames;
        attributeNames.push_back("table-id");

        RM_ScanIterator rmScanIterator;
        scan("Tables.clg", "", NO_OP, nullptr, attributeNames, rmScanIterator);
        RID dumRid;
        void* dumData = malloc(5);
        int tableNum = 0;
        while (rmScanIterator.getNextTuple(dumRid, dumData) != RBFM_EOF) {
            tableNum++;
        }
        free(dumData);
        rmScanIterator.close();
        return tableNum;
    }

    RC RelationManager::getTableId(const std::string &tableName, RID &rid, void *data) {
        std::vector<std::string> attributeNames;
        attributeNames.push_back("table-id");

        std::string conditionAttribute = "table-name";

        unsigned varCharLen = tableName.size();
        void* value = malloc(sizeof(unsigned) + varCharLen);
        memcpy(value, &varCharLen, sizeof(unsigned));
        memcpy((char*) value+sizeof(unsigned), tableName.c_str(), varCharLen);

        CompOp compOp = EQ_OP;

        RM_ScanIterator rmScanIterator;

        scan("Tables.clg", conditionAttribute, compOp, value, attributeNames, rmScanIterator);
        free(value);

        if (rmScanIterator.getNextTuple(rid, data) != RBFM_EOF) {
            rmScanIterator.close();
            free(data);
            return 0;
        }
        // Didn't find table
        else {
            rmScanIterator.close();
            free(data);
            return -1;
        }
    }

    RC RelationManager::buildRecordDescriptor(int tableId, std::vector<Attribute> &attrs) {
        std::vector<std::string> attributeNames;
        attributeNames.push_back("column-name");
        attributeNames.push_back("column-type");
        attributeNames.push_back("column-length");

        std::string conditionAttribute = "table-id";

        CompOp compOp = EQ_OP;

        int* value = &tableId;

        RM_ScanIterator rmScanIterator;

        scan("Columns.clg", conditionAttribute, compOp, value, attributeNames, rmScanIterator);
        free(value);

        RID dumRid;
        void* data = malloc(62);
        while (rmScanIterator.getNextTuple(dumRid, data) != RBFM_EOF) {
            int dataPtr = 1;
            Attribute attr;

            unsigned varCharLen;
            memcpy(&varCharLen, (char*) data+dataPtr, sizeof(unsigned));
            dataPtr += sizeof(unsigned);
            char* columnName = (char*) malloc(varCharLen);
            memcpy(columnName, (char*) data+dataPtr, varCharLen);
            dataPtr += varCharLen;
            attr.name = std::string(columnName, varCharLen);
            free(columnName);

            AttrType columnType;
            memcpy(&columnType, (char*) data+dataPtr, sizeof(AttrType));
            dataPtr += sizeof(int);
            attr.type = columnType;

            AttrLength columnLength;
            memcpy(&columnLength, (char*) data+dataPtr, sizeof(AttrLength));
            attr.length = columnLength;

            attrs.push_back(attr);
        }
        rmScanIterator.close();
        free(data);
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

} // namespace PeterDB