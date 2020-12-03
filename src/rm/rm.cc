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

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        this->rbfm = &rbfm;
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

        char* data = (char*) malloc(1+INT_SIZE);
        RID TablesRid;
        RC errCode = getTableId(tableName, TablesRid, data);
        if (errCode != 0) return errCode;

        int tableId;
        memcpy(&tableId, (char*) data+1, INT_SIZE);

        // Delete record from Tables table
        FileHandle tablesFileHandle;
        errCode = rbfm->openFile("Tables", tablesFileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->deleteRecord(tablesFileHandle, tablesRecordDescriptor, TablesRid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(tablesFileHandle);
        if (errCode != 0) return errCode;

        // Delete records form Columns table
        int* value = &tableId;

        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("table-id");

        RM_ScanIterator rmScanIterator;
        errCode = scan("Columns", "table-id", EQ_OP, value, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        FileHandle columnsFileHandle;
        errCode = rbfm->openFile("Columns", columnsFileHandle);
        if (errCode != 0) return errCode;

        RID ColumnsRid;
        while (rmScanIterator.getNextTuple(ColumnsRid, data) != RM_EOF) {
            errCode = rbfm->deleteRecord(columnsFileHandle, tablesRecordDescriptor, ColumnsRid);
            if (errCode != 0) return errCode;
        }
        rmScanIterator.close();
        free(data);

        errCode = rbfm->destroyFile(tableName);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (tableName == "Tables") attrs = tablesRecordDescriptor;
        else if (tableName == "Columns") attrs = columnsRecordDescriptor;
        else {
            void* data = malloc(1+INT_SIZE);
            RID dumRid;
            RC errCode = getTableId(tableName, dumRid, data);
            if (errCode != 0) return errCode;

            int tableId;
            memcpy(&tableId, (char*) data+1, INT_SIZE);
            free(data);

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

        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (tableName == "Tables" || tableName == "Columns") return -1;

        std::vector<Attribute> recordDescriptor;
        RC errCode = getAttributes(tableName, recordDescriptor);
        if (errCode != 0) return errCode;

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

        FileHandle fileHandle;
        errCode = rbfm->openFile(tableName, fileHandle);
        if (errCode != 0) return errCode;

        errCode = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
        if (errCode != 0) return errCode;

        errCode = rbfm->closeFile(fileHandle);
        if (errCode != 0) return errCode;

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

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator) {
        return -1;
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

        char* recordBuffer = (char*) malloc(TAB_COL_NULL_SIZE+4*INT_SIZE+VC_LEN_SIZE+TAB_COL_VC_LEN);
        int column_position = 1;
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
        if (errCode != 0) return -1;

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

    RC RelationManager::getTableId(const std::string &tableName, RID &rid, void* data) {
        unsigned varCharLen = tableName.size();
        void* value = malloc(VC_LEN_SIZE + varCharLen);
        memcpy(value, &varCharLen, VC_LEN_SIZE);
        memcpy((char*) value + VC_LEN_SIZE, tableName.c_str(), varCharLen);

        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("table-id");

        RM_ScanIterator rmScanIterator;
        RC errCode = scan("Tables", "table-name", EQ_OP, value, attributeNames, rmScanIterator);
        if (errCode != 0) return errCode;

        if (rmScanIterator.getNextTuple(rid, data) != RM_EOF) {
            rmScanIterator.close();
            free(value);
            return 0;
        }
        // Didn't find table
        else {
            rmScanIterator.close();
            free(value);
            return -1;
        }
    }

    RC RelationManager::buildRecordDescriptor(int tableId, std::vector<Attribute> &attrs) {
        int* value = &tableId;

        std::vector<std::string> attributeNames;
        attributeNames.emplace_back("column-name");
        attributeNames.emplace_back("column-type");
        attributeNames.emplace_back("column-length");

        RM_ScanIterator rmScanIterator;
        RC errCode = scan("Columns", "table-id", EQ_OP, value, attributeNames, rmScanIterator);
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
        return -1;
    }

    RC RM_IndexScanIterator::close() {
        return -1;
    }

} // namespace PeterDB