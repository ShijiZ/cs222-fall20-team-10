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
        short recordLength = getRecordLength(recordDescriptor, data);
        //std::cout << "Inside insertRecord: Record length is " << recordLength << std::endl;

        void* recordBuffer = malloc(recordLength);
        generateRecord(recordDescriptor, data, recordBuffer);

        void* pageBuffer = malloc(PAGE_SIZE);
        unsigned numPages = fileHandle.getNumberOfPages();
        unsigned pageToBeWritten = 0;
        short recordOffset = 0;
        //std::cout << "Inside insertRecord: pageNum is " << pageNum << std::endl;
        bool recordInserted = false;
        if (numPages > 0) {
            pageToBeWritten = numPages-1; // ID of page to be written
            fileHandle.readPage(pageToBeWritten, pageBuffer);
            unsigned short freeBytes = getFreeBytes(pageBuffer);
            //std::cout << "Inside inserRecode(): (1) freeBytes: " << freeBytes << std::endl;
            //std::cout << "Inside inserRecode(): (1) bytesNeeded: " << bytesNeeded << std::endl;
            short bytesNeeded = recordLength;
            if (!hasEmptySlot(pageBuffer)) {
                bytesNeeded = recordLength+2*sizeof(short);
            }

            if (freeBytes >= bytesNeeded) {
                recordInserted = insertRecordToPage(recordBuffer, recordOffset, recordLength, pageBuffer);
            }
            else {
                for (pageToBeWritten = 0; pageToBeWritten < numPages-1; pageToBeWritten++) {
                    fileHandle.readPage(pageToBeWritten, pageBuffer);
                    freeBytes = getFreeBytes(pageBuffer);
                    //std::cout << "Inside inserRecode(): (2) freeBytes: " << freeBytes << std::endl;
                    //std::cout << "Inside inserRecode(): (2) bytesNeeded: " << bytesNeeded << std::endl;
                    bytesNeeded = recordLength;
                    if (!hasEmptySlot(pageBuffer)) {
                        bytesNeeded = recordLength+2*sizeof(short);
                    }

                    if (freeBytes >= bytesNeeded) {
                        recordInserted = insertRecordToPage(recordBuffer, recordOffset, recordLength, pageBuffer);
                        break;
                    }
                }
            }
        }
        if (!recordInserted) {
            initNewPage(recordBuffer, recordLength, pageBuffer);
            //std::cout << "Inside insertRecord: after initNewpage" << std::endl;
            fileHandle.appendPage(pageBuffer);
            pageToBeWritten = numPages;
        }
        free(recordBuffer);

        rid.pageNum = pageToBeWritten;
        rid.slotNum = reuseOrInsertSlot(recordOffset, recordLength, pageBuffer);

        fileHandle.writePage(pageToBeWritten, pageBuffer);
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        unsigned numPages = fileHandle.getNumberOfPages();
        if (rid.pageNum >= numPages){
            return -1;
        }
        void* pageBuffer = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageBuffer);
        short numSlots;
        memcpy(&numSlots, (char*)pageBuffer + PAGE_SIZE - 2*sizeof(short), sizeof(short));
        if (rid.slotNum >= numSlots){
            free(pageBuffer);
            return -1;
        }

        short recordOffset;
        short recordLength;
        RID newRid = rid;
        findRecord(fileHandle, pageBuffer,recordOffset, recordLength, newRid);

        // record already deleted
        if (recordOffset == -1) {
            free(pageBuffer);
            return -1;
        }

        // generate null indicator
        unsigned numAttrs = recordDescriptor.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);

        char* nullIndicator = new char[nullIndicatorSize];
        int attrOffPtr = recordOffset + sizeof(unsigned);
        int attrCounter = 0;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            char init = 0;
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                int attrOffset;
                memcpy(&attrOffset, (char *) pageBuffer + attrOffPtr, sizeof(int));
                attrOffPtr += sizeof(int);
                //std::cout << "attrOffset is " << attrOffset << std::endl;
                if (attrOffset == -1) {
                    init += pow(2, 7-bitIndex);
                }
                attrCounter++;
            }
            nullIndicator[byteIndex] = init;
            //std::cout << "inside readRecord, init is "<< (int)init<< std::endl;
        }
        //std::cout << "inside readRecord nullIndicator is "<< nullIndicator[0] << std::endl;
        memcpy((char*)data, nullIndicator, nullIndicatorSize);
        delete[] nullIndicator;

        // read record
        unsigned attrDirSize = numAttrs*sizeof(int);
        memcpy((char*)data + nullIndicatorSize,
               (char*)pageBuffer + recordOffset + sizeof(unsigned) + attrDirSize,
               recordLength - attrDirSize - sizeof(unsigned));
        //std::cout << "Inside readRecord, null byte size within page = " << NullByteSize << std::endl;
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        unsigned numPages = fileHandle.getNumberOfPages();
        if (rid.pageNum >= numPages){
            return -1;
        }
        void* pageBuffer = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageBuffer);
        short numSlots;
        memcpy(&numSlots, (char*)pageBuffer + PAGE_SIZE - 2*sizeof(short), sizeof(short));
        if (rid.slotNum >= numSlots){
            free(pageBuffer);
            return -1;
        }

        short recordOffset;
        short recordLength;
        RID newRid = rid;
        findRecord(fileHandle, pageBuffer,recordOffset, recordLength, newRid);

        // record already deleted
        if (recordOffset == -1) {
            free(pageBuffer);
            return -1;
        }

        // shift all records after
        shiftRecord(pageBuffer, recordOffset, recordLength, -recordLength);

        // label record as deleted
        recordOffset = -1;
        memcpy((char*) pageBuffer+PAGE_SIZE-(2*newRid.slotNum+4)*sizeof(short), &recordOffset, sizeof(short));

        // update freeBytes
        unsigned short freeBytes = getFreeBytes(pageBuffer);
        unsigned short newFreeBytes = freeBytes+recordLength;
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(short), &newFreeBytes, sizeof(short));

        // write page back to disk
        fileHandle.writePage(newRid.pageNum, pageBuffer);
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        unsigned numAttrs = recordDescriptor.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        auto * nullIndicator = (unsigned char*)malloc(nullIndicatorSize);

        int attrCounter = 0;
        memcpy(nullIndicator, (char*) data, nullIndicatorSize);
        int attrPtr = nullIndicatorSize;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                bool NullBit = nullIndicator[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                Attribute attr = recordDescriptor[attrCounter];
                out << attr.name << ": ";
                if (!NullBit){
                    if (attr.type == TypeVarChar) {
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen,(char*) data + attrPtr, sizeof(unsigned));
                        attrPtr += sizeof(unsigned);
                        char *varCharBuffer = new char[varCharLen];
                        memcpy(varCharBuffer, (char *) data + attrPtr, varCharLen);
                        attrPtr += varCharLen;
                        out << std::string(varCharBuffer, varCharLen) << ", ";
                        delete[]varCharBuffer;
                    }
                    else if (attr.type == TypeInt){
                        int intAttr = 0;
                        memcpy(&intAttr,(char*) data + attrPtr, sizeof(int));
                        attrPtr += sizeof(int);
                        out << intAttr << ", ";
                    }
                    else{
                        float floatAttr = 0.0;
                        memcpy(&floatAttr,(char*)data + attrPtr, sizeof(float));
                        attrPtr += sizeof(float);
                        out << floatAttr << ", ";
                    }
                }
                else{
                    out << "NULL" << ", ";
                }
                attrCounter++;
            }
        }
        out << std::endl;
        free(nullIndicator);
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        unsigned numPages = fileHandle.getNumberOfPages();
        if (rid.pageNum >= numPages) {
            return -1;
        }
        void* pageBuffer = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageBuffer);
        short numSlots;
        memcpy(&numSlots, (char*)pageBuffer + PAGE_SIZE - 2*sizeof(short), sizeof(short));
        if (rid.slotNum >= numSlots){
            free(pageBuffer);
            return -1;
        }
        short recordOffset;
        short recordLength;
        RID newRid;
        newRid.pageNum = rid.pageNum;
        newRid.slotNum = rid.slotNum;
        findRecord(fileHandle, pageBuffer,
                     recordOffset, recordLength, newRid) ;
        if (recordOffset == -1){
            return -1;
        }
        short newRecordLen = getRecordLength(recordDescriptor, data);
        short distance = newRecordLen - recordLength;
        void* newRecordBuffer = malloc(newRecordLen);
        generateRecord(recordDescriptor, data,newRecordBuffer);
        if(distance <= 0){
            memcpy((char*)pageBuffer + recordOffset, newRecordBuffer, newRecordLen);
            shiftRecord(pageBuffer, recordOffset, recordLength, distance);
        }
        if (distance > 0){
            unsigned short freeBytes = getFreeBytes(pageBuffer);
            if (freeBytes >= distance){
                shiftRecord(pageBuffer, recordOffset, recordLength, distance);
                memcpy((char*)pageBuffer + recordOffset, newRecordBuffer, newRecordLen);
            }
            else{

            }
        }
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
    short RecordBasedFileManager::getRecordLength(const std::vector<Attribute> &recordDescriptor, const void *data) {
        const unsigned numAttrs = recordDescriptor.size();
        int nullIndicatorSize = ceil(((double) numAttrs)/8);
        char* nullIndicatorBuffer = new char[nullIndicatorSize];
        std::memcpy(nullIndicatorBuffer, data, nullIndicatorSize);

        short recordLength = sizeof(unsigned);
        char* attrPtr = (char*) data + nullIndicatorSize;
        unsigned attrCounter = 0;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                bool isNull = nullIndicatorBuffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
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
        delete[] nullIndicatorBuffer;
        return recordLength;
    }

    void RecordBasedFileManager::generateRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                                void *recordBuffer) {
        // Write numAttrs
        const unsigned numAttrs = recordDescriptor.size();
        std::memcpy(recordBuffer, &numAttrs, sizeof(unsigned));

        // Get null info bytes
        unsigned nullIndicatorSize = ceil(((double) numAttrs)/8);
        char* nullIndicatorBuffer = new char[nullIndicatorSize];
        std::memcpy(nullIndicatorBuffer, data, nullIndicatorSize);
        //std::cout << "Inside generateRecord, nullInfoByte within page = " << nullIndicatorBuffer<< std::endl;

        // Create required pointers
        int* offsetDirPtr = (int*) recordBuffer + 1;
        char* attrWritePtr = (char*) recordBuffer + (1+numAttrs)*sizeof(unsigned);
        char* attrReadPtr = (char*) data + nullIndicatorSize;

        unsigned attrCounter = 0;
        unsigned attrOffset = 0;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                // Determine if attribute is null
                bool attrIsNull = nullIndicatorBuffer[byteIndex] & (short) 1 << (short) (7 - bitIndex);
                //std::cout << "The " << attrCounter << "th attr is null? " << attrIsNull << std::endl;
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
                        memcpy(offsetDirPtr, &attrOffset, sizeof(int));
                        attrOffset += sizeof(unsigned) + varCharLen;
                        offsetDirPtr += 1;

                        // write length info and attribute to attribute region
                        memcpy(attrWritePtr, &varCharLen, sizeof(unsigned));
                        memcpy(attrWritePtr+sizeof(unsigned), attrReadPtr, varCharLen);
                        attrWritePtr += sizeof(unsigned) + varCharLen;
                        attrReadPtr += varCharLen;
                    }
                    // Attribute is of type int or real
                    else {
                        // write offset info to offsetDir
                        memcpy(offsetDirPtr, &attrOffset, sizeof(unsigned));
                        attrOffset += 4;
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
                    int attrOffset = -1;
                    memcpy(offsetDirPtr, &attrOffset, sizeof(int));
                    offsetDirPtr += 1;
                }
                attrCounter++;
            }
        }
        delete[] nullIndicatorBuffer;
    }

    unsigned short RecordBasedFileManager::getNumSlots(void* pageBuffer) {
        unsigned short numSlots;
        memcpy(&numSlots, (char*) pageBuffer+PAGE_SIZE-2*sizeof(short), sizeof(short));
        return numSlots;
    };

    unsigned short RecordBasedFileManager::getFreeBytes(void* pageBuffer) {
        unsigned short freeBytes;
        memcpy(&freeBytes, (char*) pageBuffer+PAGE_SIZE-sizeof(short), sizeof(short));
        return freeBytes;
    };

    short RecordBasedFileManager::getInsertStartOffset(void* pageBuffer) {
        unsigned short numSlots = getNumSlots(pageBuffer);

        short currRecordOffset;
        short maxRecordOffset = 0;
        short recordLenWithMaxOffset = 0;

        // find maxRecordOffset and its record length
        for (short slotNum = 0; slotNum < numSlots; slotNum++) {
            memcpy(&currRecordOffset, (char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), sizeof(short));
            if (currRecordOffset >= maxRecordOffset) {
                memcpy(&recordLenWithMaxOffset, (char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), sizeof(short));
                maxRecordOffset = currRecordOffset;
            }
        }
        return maxRecordOffset + recordLenWithMaxOffset;
    }

    bool RecordBasedFileManager::hasEmptySlot(void* pageBuffer) {
        unsigned short numSlots = getNumSlots(pageBuffer);
        short currRecordOffset;
        for (short slotNum = 0; slotNum < numSlots; slotNum++) {
            memcpy(&currRecordOffset, (char*) pageBuffer + PAGE_SIZE - (2*slotNum+4)*sizeof(short), sizeof(short));
            if (currRecordOffset == -1) {
                return true;
            }
        }
        return false;
    }

    void RecordBasedFileManager::initNewPage(void* recordBuffer, unsigned recordLength, void* pageBuffer) {
        memset(pageBuffer, 0, PAGE_SIZE);
        memcpy(pageBuffer, recordBuffer, recordLength);
        unsigned short freeBytes = PAGE_SIZE-recordLength-2*sizeof(short);
        unsigned short numSlots = 0;
        memcpy((char*) pageBuffer+PAGE_SIZE-2*sizeof(short), &numSlots, sizeof(short));
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(short), &freeBytes, sizeof(short));
    }

    bool RecordBasedFileManager::insertRecordToPage(void* recordBuffer, short& recordOffset, short recordLength, void* pageBuffer) {
        recordOffset = getInsertStartOffset(pageBuffer);
        //std::cout << "Inside insertRecordToPage: insertStartOffset is " << recordOffset << std::endl;
        memcpy((char*) pageBuffer+recordOffset, recordBuffer, recordLength);

        // set new freeBytes
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-recordLength;
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(short), &newFreeBytes, sizeof(short));
        return true;
    }

    short RecordBasedFileManager::reuseOrInsertSlot(short recordOffset, short recordLength, void* pageBuffer) {
        unsigned short numSlots = getNumSlots(pageBuffer);

        if (numSlots != 0) {
            short currRecordOffset;
            // find maxRecordOffset and its record length
            for (short slotNum = 0; slotNum < numSlots; slotNum++) {
                memcpy(&currRecordOffset, (char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), sizeof(short));

                // empty slot found
                if (currRecordOffset == -1) {
                    memcpy((char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), &recordOffset, sizeof(short));
                    memcpy((char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), &recordLength, sizeof(short));
                    return slotNum;
                }
            }
        }

        // Didn't find empty slot, append a new one
        memcpy((char*) pageBuffer+PAGE_SIZE-(4+2*numSlots)*sizeof(short), &recordOffset, sizeof(short));
        memcpy((char*) pageBuffer+PAGE_SIZE-(3+2*numSlots)*sizeof(short), &recordLength, sizeof(short));

        // set new slotNum and freeBytes
        unsigned short newNumSlots = numSlots + 1;
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-2*sizeof(short);
        memcpy((char*) pageBuffer+PAGE_SIZE-2*sizeof(short), &newNumSlots, sizeof(short));
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(short), &newFreeBytes, sizeof(short));
        return newNumSlots-1;
    }

    void RecordBasedFileManager::shiftRecord(void* pageBuffer, short recordOffset, short recordLength, short distance) {
        unsigned short numSlots = getNumSlots(pageBuffer);
        unsigned short sizeToBeShifted = 0;
        short currRecordOffset;
        short currRecordLength;
        for (short slotNum = 0; slotNum < numSlots; slotNum++) {
            memcpy(&currRecordOffset, (char*) pageBuffer + PAGE_SIZE - (2*slotNum + 4)*sizeof(short), sizeof(short));
            if (currRecordOffset > recordOffset) {
                // Negative distance means shift left; positive distance means shift right
                short newCurrRecordOffset = currRecordOffset+distance;

                // Update record offset in slot
                memcpy((char *) pageBuffer + PAGE_SIZE - (2 * slotNum + 4) * sizeof(short), &newCurrRecordOffset, sizeof(short));

                // Accumulate the length of  records to be shifted
                memcpy(&currRecordLength, (char*) pageBuffer + PAGE_SIZE - (2*slotNum + 3)*sizeof(short), sizeof(short));
                sizeToBeShifted += currRecordLength;
            }
        }
        memcpy((char*) pageBuffer+recordOffset+recordLength+distance, (char*) pageBuffer+recordOffset+recordLength, sizeToBeShifted);
    }

    void RecordBasedFileManager::findRecord(FileHandle &fileHandle, void *pageBuffer,
                                              short &recordOffset, short &recordLength, RID &rid) {
        // get record offset. If is -1, deleted.
        int recordOffPtr = PAGE_SIZE - 2*sizeof(short) - (2*rid.slotNum + 2)*sizeof(short);
        memcpy(&recordOffset, (char*) pageBuffer + recordOffPtr, sizeof(short));
        if (recordOffset == -1){
            return;
        }

        // get record length. If is -1, on another page
        int recordLenPtr = PAGE_SIZE - 2*sizeof(short) - (2*rid.slotNum + 1)*sizeof(short);
        memcpy(&recordLength, (char*) pageBuffer + recordLenPtr, sizeof(short));
        unsigned pageNum ;
        short slotNum;
        while (recordLength == -1){
            memcpy(&pageNum, (char*) pageBuffer + recordOffset, sizeof(unsigned));
            memcpy(&slotNum, (char*) pageBuffer + recordOffset + sizeof(unsigned), sizeof(short));
            rid.pageNum = pageNum;
            rid.slotNum = slotNum;
            fileHandle.readPage(rid.pageNum, pageBuffer);
            recordOffPtr = PAGE_SIZE - 2*sizeof(short) - (2*rid.slotNum + 2)*sizeof(short);
            memcpy(&recordOffset, (char*) pageBuffer + recordOffPtr, sizeof(short));
            if (recordOffset == -1){
                return;
            }
            recordLenPtr = PAGE_SIZE - 2*sizeof(short) - (2*rid.slotNum + 1)*sizeof(short);
            memcpy(&recordLength, (char*) pageBuffer + recordLenPtr, sizeof(short));
        }
    }


} // namespace PeterDB

