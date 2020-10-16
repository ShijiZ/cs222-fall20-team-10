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

        void* recordBuffer = malloc(recordLength);
        generateRecord(recordDescriptor, data, recordBuffer);

        void* pageBuffer = malloc(PAGE_SIZE);
        unsigned pageNum = fileHandle.getNumberOfPages();
        bool recordInserted = false;
        if (pageNum > 0) {
            fileHandle.readPage(pageNum, pageBuffer);
            unsigned short freeBytes = getFreeBytes(pageBuffer);
            if (freeBytes >= bytesNeeded) {
                recordInserted = insertRecordToPage(recordBuffer, pageBuffer);
            }
            else {
                for (int pageIndex = 0; pageIndex < pageNum-1; pageIndex++) {
                    fileHandle.readPage(pageNum, pageBuffer);
                    freeBytes = getFreeBytes(pageBuffer);
                    if (freeBytes >= bytesNeeded) {
                        recordInserted = insertRecordToPage(recordBuffer, pageBuffer);
                        break;
                    }
                }
            }
        }
        if (!recordInserted) {
            initNewPage(recordBuffer, pageBuffer);
            fileHandle.appendPage(pageBuffer);
        }
        free(recordBuffer);

        insertSlot();
        updateSlotsNumAndFreeBytes();

        fileHandle.writePage(pageNum-1, pageBuffer);
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        unsigned numberOfPages = fileHandle.getNumberOfPages();
        if (rid.pageNum >= numberOfPages){
            return -1;
        }
        void* page = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, page);
        int NPtr = PAGE_SIZE - 2*sizeof(short);
        short totSlotNum;
        memcpy(&totSlotNum, (char*)page + NPtr, sizeof(short ));
        if (rid.slotNum >= totSlotNum){
            free(page);
            return -1;
        }
        ////////// get record offset////////
        short recordOffset;
        // get offset from page directory
        int offPtr = PAGE_SIZE - 2*sizeof(short) - (2*rid.slotNum + 2)*sizeof(short);
        memcpy(&recordOffset, (char*) page + offPtr, sizeof(short));
        ////////// get record size /////////
        short recordSize;
        int sizePtr = PAGE_SIZE - 2*sizeof(short) - (2*rid.slotNum + 1)*sizeof(short);
        memcpy(&recordSize, (char*) page + offPtr, sizeof(short));
        ////////// read record ////////////
        unsigned NumFields = recordDescriptor.size();
        unsigned sizeFieldDir = NumFields*sizeof(int);
        memcpy((char*)data, (char*)page + recordOffset, sizeof(unsigned));
        memcpy((char*)data + sizeof(unsigned),
               (char*)page + recordOffset + sizeof(unsigned) + sizeFieldDir,
               recordSize - sizeFieldDir - sizeof(unsigned )  );
        free(page);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        unsigned NumFields = recordDescriptor.size();
        unsigned NullByteSize = ceil((double) NumFields / 8);
        auto * NullByte = (unsigned char*)malloc(NullByteSize);
        bool NullBit;
        Attribute attribute;
        int attrCounter = 0;
        memcpy(NullByte, (char*) data, NullByteSize);
        for (int byteIndex = 0; byteIndex < NullByteSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < NumFields; bitIndex++) {
                NullBit = NullByte[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                attribute = recordDescriptor[attrCounter];
                int attrLen = 0;
                int attrPtr = 0;
                out << attribute.name << " ";
                if (!NullBit){
                    if (attribute.type == TypeVarChar) {
                        attrPtr += sizeof(unsigned);
                        memcpy(&attrLen, data, sizeof(unsigned));
                        char *buffer = (char *) malloc(sizeof(unsigned));
                        memcpy(buffer, (char *) data + attrPtr, attrLen);
                        attrPtr += attrLen;
                        std::cout << std::string(buffer, attrLen) << "/n";
                        free(buffer);
                    }
                    else{
                        char *buffer = (char *) malloc(sizeof(unsigned));
                        memcpy(buffer,(char*)data + attrPtr, sizeof(unsigned));
                        attrPtr += sizeof(unsigned);
                        std::cout << *buffer << "/n";
                        free(buffer);
                    }
                }
                else{
                    std::cout << "NULL" << "/n";
                }
            }
                attrCounter ++;
            }
        std::cout << std::endl;
        free(NullByte);
        return 0;
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
        const unsigned attrNum = recordDescriptor.size();
        int nullInfoByte = ceil(((double) attrNum)/8);
        char* buffer = new char[nullInfoByte];
        std::memcpy(buffer, data, nullInfoByte);

        unsigned recordLength = 0;
        char* attrPtr = (char*) data + nullInfoByte;
        unsigned attrCounter = 0;
        for (int byteIndex = 0; byteIndex < nullInfoByte; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < attrNum; bitIndex++) {
                bool isNull = buffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                if (!isNull) {
                    Attribute attr = recordDescriptor[byteIndex*8 + bitIndex];
                    AttrType attrType = attr.type;
                    if (attrType == TypeVarChar) {
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, attrPtr, sizeof(unsigned));
                        recordLength += 2*sizeof(unsigned) + varCharLen;
                        attrPtr += sizeof(unsigned) + varCharLen;
                    }
                    else {
                        recordLength += 2*sizeof(unsigned);
                        attrPtr += sizeof(unsigned);
                    }
                }
                attrCounter++;
            }
        }
        delete[] buffer;
        return recordLength;
    }

    void RecordBasedFileManager::generateRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                                void *recordBuffer) {
        // Write attrNum
        const unsigned attrNum = recordDescriptor.size();
        std::memcpy(recordBuffer, &attrNum, sizeof(unsigned));

        // Get null info bytes
        unsigned nullInfoByte = ceil(((double) attrNum)/8);
        char* nullInfoBuffer = new char[nullInfoByte];
        std::memcpy(nullInfoBuffer, data, nullInfoByte);

        // Create required pointers
        int* offsetDirPtr = (int*) recordBuffer + 1;
        char* attrWritePtr = (char*) recordBuffer + (1+attrNum)*sizeof(unsigned);
        char* attrReadPtr = (char*) data + nullInfoByte;

        unsigned attrCounter = 0;
        unsigned offset = 0;
        for (int byteIndex = 0; byteIndex < nullInfoByte; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < attrNum; bitIndex++) {
                // Determine if attribute is null
                bool attrIsNull = nullInfoBuffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                if (!attrIsNull) {
                    Attribute attr = recordDescriptor[byteIndex*8 + bitIndex];
                    AttrType attrType = attr.type;

                    // Attribute is of type varChar
                    if (attrType == TypeVarChar) {
                        // get varChar length
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, attrReadPtr, sizeof(unsigned));
                        attrReadPtr += sizeof(unsigned);

                        // write offset info to offsetDir
                        offset += varCharLen;
                        memcpy(offsetDirPtr, &offset, sizeof(unsigned));
                        offsetDirPtr += 1;

                        // write length info and attribute to attribute region
                        memcpy(attrWritePtr, &varCharLen, sizeof(unsigned));
                        memcpy(attrWritePtr, attrReadPtr, varCharLen);
                        attrWritePtr += sizeof(unsigned) + varCharLen;
                        attrReadPtr += varCharLen;
                    }
                    // Attribute is of type int or real
                    else {
                        // write offset info to offsetDir
                        offset += 4;
                        memset(offsetDirPtr, offset, sizeof(unsigned));
                        offsetDirPtr += 1;

                        // write attribute to attribute region
                        memcpy(attrWritePtr, attrReadPtr, 4);
                        attrWritePtr += 4;
                        attrReadPtr += 4;
                    }
                }
                // Attribute is null
                else {
                    // write offset info to offsetDir
                    memset(offsetDirPtr, -1, sizeof(int));
                    offsetDirPtr += 1;
                }
                attrCounter++;
            }
        }
        delete[] nullInfoBuffer;
    }

    unsigned short RecordBasedFileManager::getSlotsNum(void* pageBuffer) {
        unsigned short slotsNum;
        memcpy(&slotsNum, (char*) pageBuffer+PAGE_SIZE-2*sizeof(unsigned short), sizeof(unsigned short));
        return slotsNum;
    };

    unsigned short RecordBasedFileManager::getFreeBytes(void* pageBuffer) {
        unsigned short freeBytes;
        memcpy(&freeBytes, (char*) pageBuffer+PAGE_SIZE-sizeof(unsigned short), sizeof(unsigned short));
        return freeBytes;
    };

    unsigned short RecordBasedFileManager::getInsertStartOffset(void* pageBuffer) {
        unsigned short slotsNum = getSlotsNum(pageBuffer);
        unsigned short insertStartOffset;
        unsigned short lastRecordLen;
        unsigned short lastRecordLen;
        memcpy(&insertStartOffset, (char*) pageBuffer+PAGE_SIZE-(1+slotsNum*2)*sizeof(unsigned short), sizeof(unsigned short));
        return insertStartOffset;
    };

    bool RecordBasedFileManager::insertRecordToPage(void* recordBuffer, void* pageBuffer) {
        unsigned short insertStartOffset = getInsertStartOffset(pageBuffer);

        return true;
    }

} // namespace PeterDB

