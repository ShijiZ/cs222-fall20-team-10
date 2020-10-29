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
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC errCode = rbfm->createFile("Tables.clg");
        if (errCode != 0) {
            return errCode;
        }
        errCode = rbfm->createFile("Columns.clg");
        if (errCode != 0) {
            return errCode;
        }

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC errCode = rbfm->destroyFile("Tables.clg");
        if (errCode != 0) {
            return errCode;
        }
        errCode = rbfm->destroyFile("Columns.clg");
        if (errCode != 0) {
            return errCode;
        }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RC errCode = rbfm->createFile(tableName + ".tbl");
        if (errCode != 0) {
            return errCode;
        }
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
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