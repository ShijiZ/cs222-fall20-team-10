#include "src/include/rbfm.h"
#include <bitset>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.openFile(fileName,fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        unsigned recordLength = getRecordLength(recordDescriptor, data);
        unsigned bytesNeeded = recordLength + 2*sizeof(short);

        unsigned pageNum = fileHandle.getNumberOfPages();
        if (pageNum == 0) {

        }
        else {
            unsigned short freeBytes = getFreeBytes();
            if (freeBytes < bytesNeeded) {
                // TODO: For LOOP
            }
        }
        insertRecordData();
        insertSlot();
        updateSlotsNumAndFreeBytes();

        fileHandle.writePage(PageNum, pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        dahai;
        return -1;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    unsigned RecordBasedFileManager::getRecordLength(const std::vector<Attribute> &recordDescriptor, const void *data) {
        const int attrNum = recordDescriptor.size();
        int nullInfoByte = ceil(((double) attrNum)/8);
        char* buffer = new char[nullInfoByte];
        std::memcpy(buffer, data, nullInfoByte);

        unsigned recordLength = 0;
        char* attrPtr = (char*) data + nullInfoByte;
        for (int byteIndex = 0; byteIndex < attrNum; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8; bitIndex++) {
                bool isNull = buffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                if (!isNull) {
                    Attribute attr = recordDescriptor[byteIndex*8 + bitIndex];
                    AttrType attrType = attr.type;
                    if (attrType == TypeVarChar) {
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, attrPtr, sizeof(unsigned));
                        recordLength += sizeof(unsigned) + varCharLen;
                        attrPtr += sizeof(unsigned) + varCharLen;
                    }
                    else {
                        recordLength += 4;
                        attrPtr += 4;
                    }
                }
            }
        }
        delete[] buffer;
        return recordLength;
    }

} // namespace PeterDB

