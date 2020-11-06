#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() {
        PagedFileManager& pfm = PagedFileManager::instance();
        this->pfm = &pfm;
    }

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;


    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return pfm->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pfm->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return pfm->openFile(fileName,fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return pfm->closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void* data, RID &rid) {
        short recordLength = generateRecordLength(recordDescriptor, data);
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
            short bytesNeeded = recordLength;
            if (!hasEmptySlot(pageBuffer)) bytesNeeded = recordLength + REC_OFF_SIZE + REC_LEN_SIZE;

            //std::cout << "Inside insertRecord(): (1) freeBytes: " << freeBytes << std::endl;
            //std::cout << "Inside insertRecord(): (1) bytesNeeded: " << bytesNeeded << std::endl;
            if (freeBytes >= bytesNeeded) {
                recordInserted = insertRecordToPage(recordBuffer, recordOffset, recordLength, pageBuffer);
            }
            else {
                for (pageToBeWritten = 0; pageToBeWritten < numPages-1; pageToBeWritten++) {
                    fileHandle.readPage(pageToBeWritten, pageBuffer);
                    freeBytes = getFreeBytes(pageBuffer);
                    //std::cout << "Inside insertRecord(): (2) freeBytes: " << freeBytes << std::endl;
                    //std::cout << "Inside insertRecord(): (2) bytesNeeded: " << bytesNeeded << std::endl;
                    bytesNeeded = recordLength;
                    if (!hasEmptySlot(pageBuffer)) bytesNeeded = recordLength + REC_OFF_SIZE + REC_LEN_SIZE;

                    if (freeBytes >= bytesNeeded) {
                        recordInserted = insertRecordToPage(recordBuffer, recordOffset, recordLength, pageBuffer);
                        break;
                    }
                }
            }
        }
        if (!recordInserted) {
            initNewPage(recordBuffer, recordLength, pageBuffer);
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
                                          const RID &rid, void* data) {
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
            free(pageBuffer);
            return errCode;
        }

        // generate null indicator
        unsigned numAttrs = recordDescriptor.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);

        char* nullIndicator = new char[nullIndicatorSize];
        int attrOffPtr = recordOffset + NUM_ATTR_SIZE;
        int attrCounter = 0;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            char init = 0;
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                int attrOffset;
                memcpy(&attrOffset, (char*) pageBuffer + attrOffPtr, ATTR_OFF_SIZE);
                attrOffPtr += ATTR_OFF_SIZE;
                if (attrOffset == -1) {
                    init += pow(2, 7-bitIndex);
                }
                attrCounter++;
            }
            nullIndicator[byteIndex] = init;
        }
        //std::cout << "inside readRecord nullIndicator is "<< nullIndicator[0] << std::endl;
        memcpy((char*)data, nullIndicator, nullIndicatorSize);
        delete[] nullIndicator;

        // read record
        unsigned attrDirSize = numAttrs*ATTR_OFF_SIZE;
        memcpy((char*)data + nullIndicatorSize,
               (char*)pageBuffer + recordOffset + NUM_ATTR_SIZE + attrDirSize,
               recordLength - NUM_ATTR_SIZE - attrDirSize);
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
            free(pageBuffer);
            return errCode;
        }

        // shift all records after
        shiftRecord(pageBuffer, recordOffset, recordLength, -recordLength);

        // label record as deleted
        setRecordOffset(pageBuffer, newRid.slotNum, -1);
        setRecordLength(pageBuffer, newRid.slotNum, 0);

        // update freeBytes
        unsigned short freeBytes = getFreeBytes(pageBuffer);
        unsigned short newFreeBytes = freeBytes+recordLength;
        setFreeBytes(pageBuffer, newFreeBytes);

        // write page back to disk
        fileHandle.writePage(newRid.pageNum, pageBuffer);
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void* data,
                                           std::ostream &out) {
        unsigned numAttrs = recordDescriptor.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        auto * nullIndicator = (unsigned char*) malloc(nullIndicatorSize);

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
                        memcpy(&varCharLen,(char*) data + attrPtr, VC_LEN_SIZE);
                        attrPtr += VC_LEN_SIZE;
                        char* varCharBuffer = new char[varCharLen];
                        memcpy(varCharBuffer, (char*) data + attrPtr, varCharLen);
                        attrPtr += varCharLen;
                        out << std::string(varCharBuffer, varCharLen) << ", ";
                        delete[]varCharBuffer;
                    }
                    else if (attr.type == TypeInt){
                        int intAttr = 0;
                        memcpy(&intAttr,(char*) data + attrPtr, INT_SIZE);
                        attrPtr += INT_SIZE;
                        out << intAttr << ", ";
                    }
                    else{
                        float floatAttr = 0.0;
                        memcpy(&floatAttr,(char*) data + attrPtr, FLT_SIZE);
                        attrPtr += FLT_SIZE;
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
                                            const void* data, const RID &rid) {
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
            free(pageBuffer);
            return errCode;
        }

        short newRecordLength = generateRecordLength(recordDescriptor, data);
        short distance = newRecordLength - recordLength;
        unsigned short freeBytes = getFreeBytes(pageBuffer);
        unsigned short newFreeBytes = freeBytes - distance;
        void* newRecordBuffer = malloc(newRecordLength);
        generateRecord(recordDescriptor, data,newRecordBuffer);
        // record can stay in current page
        if (freeBytes >= distance){
            shiftRecord(pageBuffer, recordOffset, recordLength, distance);
            memcpy((char*) pageBuffer+recordOffset, newRecordBuffer, newRecordLength);
            setFreeBytes(pageBuffer, newFreeBytes);
            setRecordLength(pageBuffer, newRid.slotNum, newRecordLength);
        }
        // record must be moved to another page
        else{
            void* newPageBuffer = malloc(PAGE_SIZE);
            unsigned numPages = fileHandle.getNumberOfPages();
            unsigned pageToBeUpdated = 0;
            short newRecordOffset = 0;
            bool recordUpdated = false;
            if (numPages > 1) {
                for (pageToBeUpdated = 0; pageToBeUpdated < numPages; pageToBeUpdated++){
                    fileHandle.readPage(pageToBeUpdated, newPageBuffer);
                    unsigned short curFreeBytes = getFreeBytes(newPageBuffer);
                    short bytesNeeded = newRecordLength;
                    if (!hasEmptySlot(newPageBuffer)) bytesNeeded = newRecordLength + REC_OFF_SIZE + REC_LEN_SIZE;

                    if (curFreeBytes >= bytesNeeded){
                        recordUpdated = insertRecordToPage(newRecordBuffer, newRecordOffset, newRecordLength, newPageBuffer);
                        break;
                    }
                }
            }
            if(!recordUpdated){
                initNewPage(newRecordBuffer, newRecordLength, newPageBuffer);
                fileHandle.appendPage(newPageBuffer);
                pageToBeUpdated = numPages;
            }

            unsigned newPageNum = pageToBeUpdated;
            unsigned short newSlotNum = reuseOrInsertSlot(newRecordOffset, newRecordLength, newPageBuffer);
            fileHandle.writePage(pageToBeUpdated, newPageBuffer);
            free(newPageBuffer);

            // set the original page (pageBuffer)
            shiftRecord(pageBuffer, recordOffset, recordLength, PTR_PN_SIZE+PTR_SN_SIZE-recordLength);
            memcpy((char*) pageBuffer+recordOffset, &newPageNum, PTR_PN_SIZE);
            memcpy((char*) pageBuffer+recordOffset+PTR_PN_SIZE, &newSlotNum, PTR_SN_SIZE);
            newFreeBytes = freeBytes-(PTR_PN_SIZE+PTR_SN_SIZE-recordLength);
            setFreeBytes(pageBuffer, newFreeBytes);

            // label record as moved
            setRecordLength(pageBuffer, newRid.slotNum, -1);
        }
        free(newRecordBuffer);
        fileHandle.writePage(newRid.pageNum, pageBuffer);
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void* data) {
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
            free(pageBuffer);
            return errCode;
        }

        int attrOffPtr = recordOffset + NUM_ATTR_SIZE;
        const unsigned numAttrs = recordDescriptor.size();
        for (int attrIndex = 0; attrIndex < numAttrs; attrIndex++) {
            Attribute attr = recordDescriptor[attrIndex];
            if (attr.name == attributeName) {
                int attrOffset;
                memcpy(&attrOffset, (char*) pageBuffer+attrOffPtr, ATTR_OFF_SIZE);

                // attribute is null
                if (attrOffset == -1) {
                    unsigned char nullIndicator = 128;  // 10000000
                    memcpy((char*)data, &nullIndicator, 1);
                }
                // attribute is not null
                else {
                    char nullIndicator = 0;  // 00000000
                    memcpy((char*)data, &nullIndicator, 1);

                    int attrPtr = recordOffset + NUM_ATTR_SIZE + numAttrs*ATTR_OFF_SIZE + attrOffset;
                    if (attr.type == TypeVarChar) {
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, (char*) pageBuffer+attrPtr, VC_LEN_SIZE);
                        memcpy((char*)data+1, (char*) pageBuffer+attrPtr, VC_LEN_SIZE+varCharLen);
                    }
                    else if (attr.type == TypeInt) {
                        memcpy((char*)data+1, (char*) pageBuffer+attrPtr, INT_SIZE);
                    }
                    else {
                        memcpy((char*)data+1, (char*) pageBuffer+attrPtr, FLT_SIZE);
                    }
                }
            }
            attrOffPtr += ATTR_OFF_SIZE;
        }
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void* value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
       return rbfm_ScanIterator.initialize(fileHandle, recordDescriptor, conditionAttribute,
                                           compOp, value, attributeNames);
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    short RecordBasedFileManager::generateRecordLength(const std::vector<Attribute> &recordDescriptor, const void* data) {
        // Get nullIndicator
        const unsigned numAttrs = recordDescriptor.size();
        int nullIndicatorSize = ceil(((double) numAttrs)/8);
        char* nullIndicatorBuffer = new char[nullIndicatorSize];
        memcpy(nullIndicatorBuffer, data, nullIndicatorSize);

        short recordLength = NUM_ATTR_SIZE;
        char* attrPtr = (char*) data + nullIndicatorSize;
        unsigned attrCounter = 0;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                // Determine if attribute is null
                bool isNull = nullIndicatorBuffer[byteIndex] & (int) 1 << (int) (7 - bitIndex);
                // Attribute is null
                if (isNull) {
                    recordLength += ATTR_OFF_SIZE;
                }
                else {
                    Attribute attr = recordDescriptor[attrCounter];
                    AttrType attrType = attr.type;
                    // Attribute is of type varChar
                    if (attrType == TypeVarChar) {
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, attrPtr, VC_LEN_SIZE);
                        recordLength += ATTR_OFF_SIZE + VC_LEN_SIZE + varCharLen;
                        attrPtr += VC_LEN_SIZE + varCharLen;
                    }
                    // Attribute is of type int
                    else if (attrType == TypeInt){
                        recordLength += ATTR_OFF_SIZE + INT_SIZE;
                        attrPtr += INT_SIZE;
                    }
                        // Attribute is of type real
                    else {
                        recordLength += ATTR_OFF_SIZE + FLT_SIZE;
                        attrPtr += FLT_SIZE;
                    }
                }
                attrCounter++;
            }
        }
        delete[] nullIndicatorBuffer;
        return recordLength;
    }

    void RecordBasedFileManager::generateRecord(const std::vector<Attribute> &recordDescriptor, const void* data,
                                                void* recordBuffer) {
        // Write numAttrs
        const unsigned numAttrs = recordDescriptor.size();
        memcpy(recordBuffer, &numAttrs, NUM_ATTR_SIZE);

        // Get nullIndicator
        unsigned nullIndicatorSize = ceil(((double) numAttrs)/8);
        char* nullIndicatorBuffer = new char[nullIndicatorSize];
        memcpy(nullIndicatorBuffer, data, nullIndicatorSize);

        // Create required pointers
        int offsetWritePtr = NUM_ATTR_SIZE;
        int attrWritePtr = NUM_ATTR_SIZE + numAttrs*ATTR_OFF_SIZE;
        int attrReadPtr = nullIndicatorSize;

        unsigned attrCounter = 0;
        unsigned attrOffset = 0;
        for (int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++) {
            for (int bitIndex = 0; bitIndex < 8 && attrCounter < numAttrs; bitIndex++) {
                // Determine if attribute is null
                bool attrIsNull = nullIndicatorBuffer[byteIndex] & (int) 1 << (int) (7 - bitIndex);
                if (!attrIsNull) {
                    Attribute attr = recordDescriptor[attrCounter];
                    AttrType attrType = attr.type;

                    // Attribute is of type varChar
                    if (attrType == TypeVarChar) {
                        // get varChar length
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, (char*) data + attrReadPtr, VC_LEN_SIZE);
                        attrReadPtr += VC_LEN_SIZE;

                        // write offset info to offsetDir
                        memcpy((char*) recordBuffer + offsetWritePtr, &attrOffset, ATTR_OFF_SIZE);
                        attrOffset += VC_LEN_SIZE + varCharLen;
                        offsetWritePtr += ATTR_OFF_SIZE;

                        // write length info and attribute to attribute region
                        memcpy((char*) recordBuffer + attrWritePtr, &varCharLen, VC_LEN_SIZE);
                        memcpy((char*) recordBuffer + attrWritePtr + VC_LEN_SIZE, (char*) data + attrReadPtr, varCharLen);
                        attrWritePtr += VC_LEN_SIZE + varCharLen;
                        attrReadPtr += varCharLen;
                    }
                    // Attribute is of type int
                    else if (attrType == TypeInt) {
                        // write offset info to offsetDir
                        memcpy((char*) recordBuffer + offsetWritePtr, &attrOffset, ATTR_OFF_SIZE);
                        attrOffset += INT_SIZE;
                        offsetWritePtr += ATTR_OFF_SIZE;

                        // write attribute to attribute region
                        memcpy((char*) recordBuffer + attrWritePtr, (char*) data + attrReadPtr, INT_SIZE);
                        attrWritePtr += INT_SIZE;
                        attrReadPtr += INT_SIZE;
                    }
                    // Attribute is of type real
                    else {
                        // write offset info to offsetDir
                        memcpy((char*) recordBuffer + offsetWritePtr, &attrOffset, ATTR_OFF_SIZE);
                        attrOffset += FLT_SIZE;
                        offsetWritePtr += ATTR_OFF_SIZE;

                        // write attribute to attribute region
                        memcpy((char*) recordBuffer + attrWritePtr, (char*) data + attrReadPtr, FLT_SIZE);
                        attrWritePtr += FLT_SIZE;
                        attrReadPtr += FLT_SIZE;
                    }
                }
                // Attribute is null
                else {
                    // write offset info to offsetDir
                    int nullAttrOffset = -1;
                    memcpy((char*) recordBuffer + offsetWritePtr, &nullAttrOffset, ATTR_OFF_SIZE);
                    offsetWritePtr += ATTR_OFF_SIZE;
                }
                attrCounter++;
            }
        }
        delete[] nullIndicatorBuffer;
    }

    short RecordBasedFileManager::getInsertStartOffset(void* pageBuffer) {
        unsigned short numSlots = getNumSlots(pageBuffer);

        short currRecordOffset;
        short maxRecordOffset = 0;
        short recordLenWithMaxOffset = 0;

        // find maxRecordOffset and its record length
        for (unsigned short slotNum = 0; slotNum < numSlots; slotNum++) {
            currRecordOffset = getRecordOffset(pageBuffer, slotNum);
            if (currRecordOffset >= maxRecordOffset) {
                recordLenWithMaxOffset = getRecordLength(pageBuffer, slotNum);
                if (recordLenWithMaxOffset == -1) {
                    recordLenWithMaxOffset = PTR_PN_SIZE + PTR_SN_SIZE;
                }
                maxRecordOffset = currRecordOffset;
            }
        }
        return maxRecordOffset + recordLenWithMaxOffset;
    }

    bool RecordBasedFileManager::hasEmptySlot(void* pageBuffer) {
        unsigned short numSlots = getNumSlots(pageBuffer);
        short currRecordOffset;
        for (unsigned short slotNum = 0; slotNum < numSlots; slotNum++) {
            currRecordOffset = getRecordOffset(pageBuffer, slotNum);
            if (currRecordOffset == -1) return true;
        }
        return false;
    }

    void RecordBasedFileManager::initNewPage(void* recordBuffer, unsigned recordLength, void* pageBuffer) {
        memset(pageBuffer, 0, PAGE_SIZE);
        memcpy(pageBuffer, recordBuffer, recordLength);
        unsigned short freeBytes = PAGE_SIZE-recordLength-N_SIZE-F_SIZE;
        unsigned short numSlots = 0;
        setNumSlots(pageBuffer, numSlots);
        setFreeBytes(pageBuffer, freeBytes);
    }

    bool RecordBasedFileManager::insertRecordToPage(void* recordBuffer, short& recordOffset, short recordLength, void* pageBuffer) {
        recordOffset = getInsertStartOffset(pageBuffer);
        memcpy((char*) pageBuffer+recordOffset, recordBuffer, recordLength);

        // set new freeBytes
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-recordLength;
        setFreeBytes(pageBuffer, newFreeBytes);
        return true;
    }

    unsigned short RecordBasedFileManager::reuseOrInsertSlot(short recordOffset, short recordLength, void* pageBuffer) {
        unsigned short numSlots = getNumSlots(pageBuffer);

        if (numSlots != 0) {
            short currRecordOffset;
            // find maxRecordOffset and its record length
            for (unsigned short slotNum = 0; slotNum < numSlots; slotNum++) {
                currRecordOffset = getRecordOffset(pageBuffer, slotNum);

                // empty slot found
                if (currRecordOffset == -1) {
                    setRecordOffset(pageBuffer, slotNum, recordOffset);
                    setRecordLength(pageBuffer, slotNum, recordLength);
                    return slotNum;
                }
            }
        }

        // Didn't find empty slot, append a new one
        setRecordOffset(pageBuffer, numSlots, recordOffset);
        setRecordLength(pageBuffer, numSlots, recordLength);

        // set new numSlots and freeBytes
        unsigned short newNumSlots = numSlots + 1;
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-REC_OFF_SIZE-REC_LEN_SIZE;
        setNumSlots(pageBuffer, newNumSlots);
        setFreeBytes(pageBuffer, newFreeBytes);
        return newNumSlots-1;
    }

    void RecordBasedFileManager::shiftRecord(void* pageBuffer, short recordOffset, short recordLength, short distance) {
        // Negative distance means shift left; positive distance means shift right
        unsigned short numSlots = getNumSlots(pageBuffer);
        unsigned short sizeToBeShifted = 0;
        short currRecordOffset;
        short currRecordLength;
        for (unsigned short slotNum = 0; slotNum < numSlots; slotNum++) {
            currRecordOffset = getRecordOffset(pageBuffer, slotNum);
            if (currRecordOffset > recordOffset) {
                // Update record offset in slot
                setRecordOffset(pageBuffer, slotNum, currRecordOffset+distance);

                // Accumulate the length of records to be shifted
                currRecordLength = getRecordLength(pageBuffer, slotNum);
                if (currRecordLength == -1) {
                    currRecordLength = PTR_PN_SIZE + PTR_SN_SIZE;
                }
                sizeToBeShifted += currRecordLength;
            }
        }
        memcpy((char*) pageBuffer+recordOffset+recordLength+distance, (char*) pageBuffer+recordOffset+recordLength, sizeToBeShifted);
    }

    RC RecordBasedFileManager::checkAndFindRecord(FileHandle &fileHandle, void* pageBuffer,
                                                  short &recordOffset, short &recordLength, RID &rid) {
        // validate pageNum
        unsigned numPages = fileHandle.getNumberOfPages();
        //std::cout<<"inside checkAndFindRecord rid.pageNum is "<<rid.pageNum<<std::endl;
        //std::cout<<"inside checkAndFindRecord numPages is "<<numPages<<std::endl;
        if (rid.pageNum >= numPages) return -1;

        fileHandle.readPage(rid.pageNum, pageBuffer);

        // validate slotNum
        unsigned short numSlots = getNumSlots(pageBuffer);
        //std::cout<<"inside checkAndFindRecord rid.slotNum is "<<rid.slotNum<<std::endl;
        //std::cout<<"inside checkAndFindRecord numSlots is "<<numSlots<<std::endl;
        if (rid.slotNum >= numSlots) return -1;

        return findRecord(fileHandle, pageBuffer,recordOffset, recordLength, rid);
    }

    RC RecordBasedFileManager::findRecord(FileHandle &fileHandle, void* pageBuffer,
                                            short &recordOffset, short &recordLength, RID &rid) {
        // get record offset. If is -1, record is already deleted.
        recordOffset = getRecordOffset(pageBuffer, rid.slotNum);
        if (recordOffset == -1) return -1;

        // get record length. If is -1, record is on another page
        recordLength = getRecordLength(pageBuffer, rid.slotNum);

        unsigned newPageNum = rid.pageNum;
        unsigned short newSlotNum = rid.slotNum;
        while (recordLength == -1) {
            memcpy(&newPageNum, (char*) pageBuffer + recordOffset, PTR_PN_SIZE);
            memcpy(&newSlotNum, (char*) pageBuffer + recordOffset + PTR_PN_SIZE, PTR_SN_SIZE);

            fileHandle.readPage(newPageNum, pageBuffer);

            // get record offset. If is -1, record is already deleted.
            recordOffset = getRecordOffset(pageBuffer, newSlotNum);
            if (recordOffset == -1) return -1;
            recordLength = getRecordLength(pageBuffer, newSlotNum);
        }

        rid.pageNum = newPageNum;
        rid.slotNum = newSlotNum;
        return 0;
    }

    /*********************************************/
    /*****    Getter and Setter functions  *******/
    /*********************************************/
    unsigned short RecordBasedFileManager::getNumSlots(void* pageBuffer) {
        unsigned short numSlots;
        memcpy(&numSlots, (char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE, N_SIZE);
        return numSlots;
    }

    unsigned short RecordBasedFileManager::getFreeBytes(void* pageBuffer) {
        unsigned short freeBytes;
        memcpy(&freeBytes, (char*) pageBuffer+PAGE_SIZE-F_SIZE, F_SIZE);
        return freeBytes;
    }

    short RecordBasedFileManager::getRecordLength(void* pageBuffer, unsigned short slotNum) {
        short recordLength;
        memcpy(&recordLength, (char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), REC_LEN_SIZE);
        return recordLength;
    }

    short RecordBasedFileManager::getRecordOffset(void* pageBuffer, unsigned short slotNum) {
        short recordOffset;
        memcpy(&recordOffset, (char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), REC_OFF_SIZE);
        return recordOffset;
    }

    void RecordBasedFileManager::setNumSlots(void* pageBuffer, unsigned short numSlots) {
        memcpy((char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE, &numSlots, N_SIZE);
    }

    void RecordBasedFileManager::setFreeBytes(void* pageBuffer, unsigned short freeBytes) {
        memcpy((char*) pageBuffer+PAGE_SIZE-F_SIZE, &freeBytes, F_SIZE);
    }

    void RecordBasedFileManager::setRecordLength(void* pageBuffer, unsigned short slotNum, short recordLength) {
        memcpy((char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), &recordLength, REC_LEN_SIZE);
    }

    void RecordBasedFileManager::setRecordOffset(void* pageBuffer, unsigned short slotNum, short recordOffset) {
        memcpy((char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), &recordOffset, REC_OFF_SIZE);
    }

    /*************************************************/
    /*****    functions of rbfm_Scan_Iterator  *******/
    /*************************************************/
    RC RBFM_ScanIterator::initialize(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                     const std::string &conditionAttribute, const CompOp compOp, const void* value,
                                     const std::vector<std::string> &attributeNames) {
        this->recordDescriptor = recordDescriptor;
        this->compOp = compOp;

        if (value == NULL || value == nullptr) this->value = nullptr;
        else this->value = value;

        this->attributeNames = attributeNames;
        this->currPageNum = 0;
        this->currSlotNum = 0;

        // initialize targetAttrIdxs
        int numAttrs = recordDescriptor.size();
        short attrIdx;
        for (short targetAttrIdx = 0; targetAttrIdx < attributeNames.size(); targetAttrIdx++){
            for (attrIdx = 0; attrIdx < numAttrs; attrIdx++){
                if (recordDescriptor[attrIdx].name == attributeNames[targetAttrIdx]){
                    this->targetAttrIdxs.push_back(attrIdx);
                    break;
                }
            }
            if (attrIdx == numAttrs) return -1;   // attributeName not found
        }

        // initialize conditionAttrIdx and conditionType
        for (attrIdx = 0; attrIdx < numAttrs; attrIdx++){
            if (recordDescriptor[attrIdx].name == conditionAttribute){
                this->conditionAttrIdx = attrIdx;
                this->conditionType = recordDescriptor[attrIdx].type;
                break;
            }
        }
        if (attrIdx == numAttrs){
            if (compOp == NO_OP) this->conditionAttrIdx = -1;
            else return -1;
        }

        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void* data) {
        void* pageBuffer = malloc(PAGE_SIZE);
        unsigned numPages = fileHandle.getNumberOfPages();
        int pageNum = 0;
        short slotNum = 0;
        short recordOffset;
        short recordLength;
        bool foundCondAttr = false;
        int numAttrs = recordDescriptor.size();
        short numSlots = -1;

        for (pageNum = currPageNum; pageNum < numPages; pageNum++){
            //std::cout<<"inside getNextRecord after enter outer for loop, currPageNum is "<<currPageNum<< ", pageNum is " << pageNum <<std::endl;
            RC errCode = fileHandle.readPage(pageNum, pageBuffer);
            if (errCode != 0) return errCode;
            numSlots = getNumSlots(pageBuffer);

            for (slotNum = currSlotNum; slotNum < numSlots; slotNum++) {
                parseRecord(pageBuffer, slotNum, recordOffset, recordLength);

                if (recordOffset == -1 || recordLength == -1) continue;
                if (conditionAttrIdx == -1){
                    foundCondAttr = true;
                    break;
                }
                else{
                    short attrOffset = 0;
                    short attrLen = 0;
                    RC errCode = parseAttr(attrLen, attrOffset, pageBuffer, recordOffset, conditionAttrIdx, numAttrs);
                    if (errCode != 0) return errCode;

                    // Conditional attribute in current record is not null
                    if (attrOffset != -1){
                        void* attrBuffer = malloc(attrLen);
                        memcpy((char*) attrBuffer, (char*) pageBuffer + recordOffset + NUM_ATTR_SIZE + numAttrs*ATTR_OFF_SIZE + attrOffset, attrLen);
                        foundCondAttr = findCondAttr(attrBuffer, attrLen);
                        free(attrBuffer);
                    }
                    // Conditional attribute in current record is null
                    else {
                        if (compOp == EQ_OP) foundCondAttr = value == nullptr;
                        else if (compOp == NE_OP) foundCondAttr = value != nullptr;
                        else return -1;
                    }
                    if (foundCondAttr) break;
                }
            }
            if (foundCondAttr) break;

            if (slotNum == numSlots) currSlotNum = 0;
        }

        if (foundCondAttr){
            rid.pageNum = pageNum;
            rid.slotNum = slotNum;

            if (slotNum == numSlots){
                currSlotNum = 0;
                currPageNum = pageNum+1;
            }
            else {
                currSlotNum = slotNum+1;
                currPageNum = pageNum;
            }

            int newNullIndicatorSize = ceil(((double) attributeNames.size())/8);
            //std::cout<<"inside getNextRecord inside if found, newNullIndicatorSize is "<< newNullIndicatorSize << std::endl;
            auto * newNullIndicator = (unsigned char*)malloc(newNullIndicatorSize);
            memset(newNullIndicator,0, newNullIndicatorSize);
            int dataPtr = newNullIndicatorSize;
            //std::cout<<"inside getNextRecord inside if found, dataPtr is "<< dataPtr<<std::endl;

            for (short i = 0; i < attributeNames.size(); i++){
                short attrLen = 0;
                short attrOffset = 0;
                RC errCode = parseAttr(attrLen, attrOffset, pageBuffer, recordOffset, targetAttrIdxs[i], numAttrs);
                if (errCode != 0) return errCode;

                // Target attribute in current record is not null
                if (attrOffset != -1){
                    memcpy((char*) data + dataPtr, (char*) pageBuffer + recordOffset + NUM_ATTR_SIZE + numAttrs*ATTR_OFF_SIZE + attrOffset, attrLen);
                    dataPtr += attrLen;
                }
                // Target attribute in current record is null
                else{
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    newNullIndicator[byteIndex] += pow(2, 7-bitIndex);
                }
            }
            free(pageBuffer);
            memcpy((char*) data, newNullIndicator, newNullIndicatorSize);
            free(newNullIndicator);
            return 0;
        }
        free(pageBuffer);
        return RBFM_EOF;
    }

    RC RBFM_ScanIterator::parseAttr(short &attrLen, short &attrOffset, void* pageBuffer, short recordOffset, short idx, int numAttrs){
        memcpy(&attrOffset, (char*) pageBuffer + recordOffset + NUM_ATTR_SIZE + idx*ATTR_OFF_SIZE, ATTR_OFF_SIZE);
        // Attribute is not null
        if (attrOffset != -1) {
            // Attribute is of type varChar
            if (recordDescriptor[idx].type == TypeVarChar){
                unsigned varCharLen = 0;
                memcpy(&varCharLen, (char*) pageBuffer + recordOffset + NUM_ATTR_SIZE + numAttrs*ATTR_OFF_SIZE + attrOffset, VC_LEN_SIZE);
                attrLen = varCharLen + VC_LEN_SIZE;
            }
            // Attribute is of type int
            else if (recordDescriptor[idx].type == TypeInt){
                attrLen = INT_SIZE;
            }
            // Attribute is of type real
            else {
                attrLen = FLT_SIZE;
            }
        }
        // Attribute is null
        else {
            attrLen = 0;
        }
        return 0;
    }

    bool RBFM_ScanIterator::findCondAttr(const void* checkAttr, short attrLen){
        bool found = false;
        // Attribute is of type varChar
        if (conditionType == TypeVarChar) {
            unsigned checkVarCharLen = attrLen - VC_LEN_SIZE;
            char* checkVarChar = (char*) malloc(checkVarCharLen);
            memcpy(checkVarChar, (char*) checkAttr + VC_LEN_SIZE, checkVarCharLen);

            unsigned valueVarCharLen;
            memcpy(&valueVarCharLen, (char*) value, VC_LEN_SIZE);
            char* valueVarChar = (char*) malloc(valueVarCharLen);
            memcpy(valueVarChar, (char*) value + VC_LEN_SIZE, valueVarCharLen);
            switch(compOp) {
                case EQ_OP:
                    found = std::string(valueVarChar, valueVarCharLen) == std::string(checkVarChar, checkVarCharLen);
                    break;
                case LT_OP:
                    found = std::string(valueVarChar, valueVarCharLen) > std::string(checkVarChar, checkVarCharLen);
                    break;
                case LE_OP:
                    found = std::string(valueVarChar, valueVarCharLen) >= std::string(checkVarChar, checkVarCharLen);
                    break;
                case GT_OP:
                    found = std::string(valueVarChar, valueVarCharLen) < std::string(checkVarChar, checkVarCharLen);
                    break;
                case GE_OP:
                    found = std::string(valueVarChar, valueVarCharLen) <= std::string(checkVarChar, checkVarCharLen);
                    break;
                case NE_OP:
                    found = std::string(valueVarChar, valueVarCharLen) != std::string(checkVarChar, checkVarCharLen);
                    break;
            }
            free(checkVarChar);
            free(valueVarChar);
        }
        // Attribute is of type int
        else if (conditionType == TypeInt) {
            int check;
            memcpy(&check, (char*) checkAttr, INT_SIZE);
            int data;
            memcpy(&data, (char*) value, INT_SIZE);
            switch(compOp) {
                case EQ_OP:
                    found = data == check;
                    break;
                case LT_OP:
                    found = data > check;
                    break;
                case LE_OP:
                    found = data >= check;
                    break;
                case GT_OP:
                    found = data < check;
                    break;
                case GE_OP:
                    found = data <= check;
                    break;
                case NE_OP:
                    found = data != check;
                    break;
            }
        }
        // Attribute is of type real
        else {
            float check;
            memcpy(&check, (char*)checkAttr, FLT_SIZE);
            float data;
            memcpy(&data, (char*)value, FLT_SIZE);
            switch(compOp) {
                case EQ_OP:
                    found = data == check;
                    break;
                case LT_OP:
                    found = data > check;
                    break;
                case LE_OP:
                    found = data >= check;
                    break;
                case GT_OP:
                    found = data < check;
                    break;
                case GE_OP:
                    found = data <= check;
                    break;
                case NE_OP:
                    found = data != check;
                    break;
            }
        }
        return found;
    }

    unsigned short RBFM_ScanIterator::getNumSlots(void* pageBuffer) {
        unsigned short numSlots;
        memcpy(&numSlots, (char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE, N_SIZE);
        return numSlots;
    }

    void RBFM_ScanIterator::parseRecord(void* pageBuffer, unsigned short slotNum, short& recordOffset, short& recordLength) {
        memcpy(&recordOffset, (char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), REC_OFF_SIZE);
        memcpy(&recordLength, (char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), REC_LEN_SIZE);
    }

    RC RBFM_ScanIterator::close(){
        targetAttrIdxs.clear();
        return fileHandle.closeFile();
    }

} // namespace PeterDB

