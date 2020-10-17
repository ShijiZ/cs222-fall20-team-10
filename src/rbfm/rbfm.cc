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
        //std::cout << "Record length is " << recordLength << std::endl;
        unsigned bytesNeeded = recordLength + 2*sizeof(short);

        void* recordBuffer = malloc(recordLength);
        generateRecord(recordDescriptor, data, recordBuffer);
        //std::cout << "after generate record " << std::endl;

        void* pageBuffer = malloc(PAGE_SIZE);
        unsigned pageNum = fileHandle.getNumberOfPages();
        unsigned pageToBeWritten = 0;
        //std::cout << "pageNum is " << pageNum << std::endl;
        bool recordInserted = false;
        if (pageNum > 0) {
            pageToBeWritten = pageNum-1;
            fileHandle.readPage(pageToBeWritten, pageBuffer);
            unsigned short freeBytes = getFreeBytes(pageBuffer);
            //std::cout << "Inside inserRecode(): (1) freeBytes: " << freeBytes << std::endl;
            //std::cout << "Inside inserRecode(): (1) bytesNeeded: " << bytesNeeded << std::endl;
            if (freeBytes >= bytesNeeded) {
                recordInserted = insertRecordToPage(recordBuffer, recordLength, pageBuffer);
            }
            else {
                for (pageToBeWritten = 0; pageToBeWritten < pageNum-1; pageToBeWritten++) {
                    fileHandle.readPage(pageToBeWritten, pageBuffer);
                    freeBytes = getFreeBytes(pageBuffer);
                    //std::cout << "Inside inserRecode(): (2) freeBytes: " << freeBytes << std::endl;
                    //std::cout << "Inside inserRecode(): (2) bytesNeeded: " << bytesNeeded << std::endl;
                    if (freeBytes >= bytesNeeded) {
                        recordInserted = insertRecordToPage(recordBuffer, recordLength, pageBuffer);
                        break;
                    }
                }
            }
        }
        if (!recordInserted) {
            //std::cout << "before initNewpage" << std::endl;
            initNewPage(recordBuffer, recordLength, pageBuffer);
            //std::cout << "after initNewpage" << std::endl;
            fileHandle.appendPage(pageBuffer);
            //std::cout << "after appendpage" << std::endl;
            pageToBeWritten = pageNum;
        }
        free(recordBuffer);

        rid.pageNum = pageToBeWritten;
        rid.slotNum = insertSlot(recordLength, pageBuffer)-1;

        fileHandle.writePage(pageToBeWritten, pageBuffer);
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
        memcpy(&totSlotNum, (char*)page + NPtr, sizeof(short));
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
        memcpy(&recordSize, (char*) page + sizePtr, sizeof(short));
        ////////// generate null byte ////////
        unsigned NumFields = recordDescriptor.size();
        unsigned NullByteSize = ceil((double) NumFields / 8);

        char* NullByte = new char[NullByteSize];
        int FieldOffPtr = sizeof(unsigned);
        for (int byteIndex = 0; byteIndex < NullByteSize; byteIndex++) {
            double init = 0;
            for (int bitIndex = 0; bitIndex < 8; bitIndex++) {
                int buffer;
                memcpy(&buffer, (char *) page + FieldOffPtr, sizeof(int));
                FieldOffPtr += sizeof(int);
                if (buffer == -1) {
                    init += pow(2, 7-bitIndex);
                }
            }
            NullByte[byteIndex] = init;
        }
        //std::cout<<NullByte[0]<<std::endl;
        ////////// read record ////////////
        unsigned sizeFieldDir = NumFields*sizeof(int);
        memcpy((char*)data, NullByte, NullByteSize);
        delete[] NullByte;
        memcpy((char*)data + NullByteSize,
               (char*)page + recordOffset + sizeof(unsigned) + sizeFieldDir,
               recordSize - sizeFieldDir - sizeof(unsigned));
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

        Attribute attribute;
        int attrCounter = 0;
        memcpy(NullByte, (char*) data, NullByteSize);
        int attrPtr = NullByteSize;
        //std::cout<<"before for loop"<<std::endl;
        for (int byteIndex = 0; byteIndex < NullByteSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < NumFields; bitIndex++) {
                bool NullBit = NullByte[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                attribute = recordDescriptor[attrCounter];
                out << attribute.name << ": ";
                //std::cout<<"before if of " << attrCounter <<std::endl;
                if (!NullBit){
                    if (attribute.type == TypeVarChar) {
                        unsigned attrLen;
                        memcpy(&attrLen,(char*) data + attrPtr, sizeof(unsigned));
                        //std::cout << "Inside printRecord, varCharLen = " << attrLen << std::endl;
                        attrPtr += sizeof(unsigned);
                        char *buffer = new char[attrLen];
                        memcpy(buffer, (char *) data + attrPtr, attrLen);
                        //std::cout << "Inside printRecord, varChar content = " << std::string(buffer, attrLen) << std::endl;
                        attrPtr += attrLen;
                        out << std::string(buffer, attrLen) << ", ";

                        delete[]buffer;
                    }
                    else if (attribute.type == TypeInt){
                        int buffer;
                        memcpy(&buffer,(char*) data + attrPtr, sizeof(int));
                        attrPtr += sizeof(int);
                        out << buffer << ", ";
                    }
                    else{
                        float buffer;
                        memcpy(&buffer,(char*) data + attrPtr, sizeof(float));
                        attrPtr += sizeof(float);
                        out << buffer << ", ";
                    }
                }
                else{
                    out << "NULL" << ", ";
                }
                attrCounter++;
            }
        }
        //std::cout<<"after for loop"<<std::endl;
        out << std::endl;
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

        unsigned recordLength = sizeof(unsigned);
        char* attrPtr = (char*) data + nullInfoByte;
        unsigned attrCounter = 0;
        for (int byteIndex = 0; byteIndex < nullInfoByte; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < attrNum; bitIndex++) {
                bool isNull = buffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                if (isNull) {
                    recordLength += sizeof(unsigned);
                }
                else {
                    Attribute attr = recordDescriptor[attrCounter];
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
        unsigned attrOffset = 0;
        for (int byteIndex = 0; byteIndex < nullInfoByte; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < attrNum; bitIndex++) {
                // Determine if attribute is null
                bool attrIsNull = nullInfoBuffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                if (!attrIsNull) {
                    Attribute attr = recordDescriptor[attrCounter];
                    AttrType attrType = attr.type;

                    // Attribute is of type varChar
                    if (attrType == TypeVarChar) {
                        // get varChar length
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, attrReadPtr, sizeof(unsigned));
                        attrReadPtr += sizeof(unsigned);

                        // write offset info to offsetDir
                        attrOffset += sizeof(unsigned) + varCharLen;
                        memcpy(offsetDirPtr, &attrOffset, sizeof(int));
                        offsetDirPtr += 1;
                        // write length info and attribute to attribute region
                        memcpy(attrWritePtr, &varCharLen, sizeof(unsigned));
                        memcpy(attrWritePtr+sizeof(unsigned), attrReadPtr, varCharLen);
                        ///////////debug//////
                        //std::cout<<"VarCharLen is " <<varCharLen<<std::endl;

                        //std::cout<<"varChar content is " << std::string(attrReadPtr,varCharLen)<<std::endl;
                        /////////////////////
                        attrWritePtr += sizeof(unsigned) + varCharLen;
                        attrReadPtr += varCharLen;
                    }
                    // Attribute is of type int or real
                    else {
                        // write offset info to offsetDir
                        attrOffset += 4;
                        memset(offsetDirPtr, attrOffset, sizeof(unsigned));
                        offsetDirPtr += 1;

                        // write attribute to attribute region
                        memcpy(attrWritePtr, attrReadPtr, 4);
                        ///////////debug//////
                        //if (attrType == TypeInt) {
                        //    int debug = 0;
                        //    memcpy(&debug, attrReadPtr, sizeof(int));
                        //    std::cout<<"int content is " << debug<<std::endl;
                        //}
                        //else {
                        //    float debug = 0.0;
                        //    memcpy(&debug, attrReadPtr, sizeof(float));
                        //    std::cout<<"float content is " << debug<<std::endl;
                        //}
                        /////////////////////
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
            //std::cout << "after generate record for inner loop " << std::endl;
        }
        //std::cout << "after generate record for outer loop " << std::endl;
        delete[] nullInfoBuffer;
        //std::cout << "after delete NullInfoBuffer " << std::endl;
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

        unsigned short lastRecordOffset;
        unsigned short lastRecordLen;
        memcpy(&lastRecordOffset, (char*) pageBuffer+PAGE_SIZE-(2+slotsNum*2)*sizeof(unsigned short), sizeof(unsigned short));
        memcpy(&lastRecordLen, (char*) pageBuffer+PAGE_SIZE-(1+slotsNum*2)*sizeof(unsigned short), sizeof(unsigned short));

        return lastRecordOffset + lastRecordLen;
    };

    void RecordBasedFileManager::initNewPage(void* recordBuffer, unsigned recordLength, void* pageBuffer) {
        memcpy(pageBuffer, recordBuffer, recordLength);
        unsigned short freeBytes = PAGE_SIZE-recordLength-2*sizeof(unsigned short);
        unsigned short slotsNum = 0;
        memcpy((char*) pageBuffer+PAGE_SIZE-2*sizeof(unsigned short), &slotsNum, sizeof(unsigned short));
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(unsigned short), &freeBytes, sizeof(unsigned short));
    }

    bool RecordBasedFileManager::insertRecordToPage(void* recordBuffer, unsigned recordLength, void* pageBuffer) {
        unsigned short insertStartOffset = getInsertStartOffset(pageBuffer);
        memcpy((char*) pageBuffer+insertStartOffset, recordBuffer, recordLength);

        // set new freeBytes
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-recordLength;
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(unsigned short), &newFreeBytes, sizeof(unsigned short));
        return true;
    }

    unsigned short RecordBasedFileManager::insertSlot(unsigned recordLength, void* pageBuffer) {
        unsigned short slotsNum = getSlotsNum(pageBuffer);
        unsigned short recordOffset = 0;

        if (slotsNum != 0) {
            unsigned short prevOffset;
            unsigned short prevLen;
            memcpy(&prevOffset, (char*) pageBuffer+PAGE_SIZE-(2+2*slotsNum)*sizeof(unsigned short), sizeof(unsigned short));
            memcpy(&prevLen, (char*) pageBuffer+PAGE_SIZE-(1+2*slotsNum)*sizeof(unsigned short), sizeof(unsigned short));
            recordOffset = prevOffset + prevLen;
        }

        memcpy((char*) pageBuffer+PAGE_SIZE-(4+2*slotsNum)*sizeof(unsigned short), &recordOffset, sizeof(unsigned short));
        memcpy((char*) pageBuffer+PAGE_SIZE-(3+2*slotsNum)*sizeof(unsigned short), &recordLength, sizeof(unsigned short));

        // set new slotNum and freeBytes
        unsigned short newSlotsNum = slotsNum + 1;
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-2*sizeof(unsigned short);
        memcpy((char*) pageBuffer+PAGE_SIZE-2*sizeof(unsigned short), &newSlotsNum, sizeof(unsigned short));
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(unsigned short), &newFreeBytes, sizeof(unsigned short));

        return newSlotsNum;
    }

} // namespace PeterDB

