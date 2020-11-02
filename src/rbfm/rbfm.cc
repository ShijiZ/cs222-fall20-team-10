#include "src/include/rbfm.h"
#include <bitset>

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
                                            const void *data, RID &rid) {
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
            //std::cout << "Inside insertRecord(): (1) freeBytes: " << freeBytes << std::endl;
            //std::cout << "Inside insertRecord(): (1) bytesNeeded: " << bytesNeeded << std::endl;
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
                    //std::cout << "Inside insertRecord(): (2) freeBytes: " << freeBytes << std::endl;
                    //std::cout << "Inside insertRecord(): (2) bytesNeeded: " << bytesNeeded << std::endl;
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
            std::cout << "Inside insertRecord: after initNewpage" << std::endl;
            fileHandle.appendPage(pageBuffer);
            pageToBeWritten = numPages;

            ///// debug
            numPages = fileHandle.getNumberOfPages();
            std::cout << "Inside insertRecord: after initNewpage, numPages is " << numPages << std::endl;
            ///// debug
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
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
            return errCode;
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
                memcpy(&attrOffset, (char*) pageBuffer + attrOffPtr, sizeof(int));
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
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        std::cout << "inside deleteRecord, errCode is " << errCode << std::endl;
        if (errCode != 0) {
            return errCode;
        }

        // shift all records after
        shiftRecord(pageBuffer, recordOffset, recordLength, -recordLength);

        // label record as deleted
        setRecordOffset(pageBuffer, newRid.slotNum, -1);

        // update freeBytes
        unsigned short freeBytes = getFreeBytes(pageBuffer);
        unsigned short newFreeBytes = freeBytes+recordLength;
        setFreeBytes(pageBuffer, newFreeBytes);

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
                        char* varCharBuffer = new char[varCharLen];
                        memcpy(varCharBuffer, (char*) data + attrPtr, varCharLen);
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
                        memcpy(&floatAttr,(char*) data + attrPtr, sizeof(float));
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
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
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
                for (pageToBeUpdated = 0; pageToBeUpdated < numPages-1; pageToBeUpdated++){
                    fileHandle.readPage(pageToBeUpdated, newPageBuffer);
                    unsigned short curFreeBytes = getFreeBytes(newPageBuffer);
                    short bytesNeeded = newRecordLength;
                    if (!hasEmptySlot(newPageBuffer)){
                        bytesNeeded = newRecordLength + 2*sizeof(short);
                    }
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
            short newSlotNum = reuseOrInsertSlot(newRecordOffset, newRecordLength, newPageBuffer);
            fileHandle.writePage(pageToBeUpdated, newPageBuffer);
            free(newPageBuffer);

            // set the original page (pageBuffer)
            memcpy((char*) pageBuffer+recordOffset, &newPageNum, sizeof(unsigned));
            memcpy((char*) pageBuffer+recordOffset+sizeof(unsigned), &newSlotNum, sizeof(short));
            shiftRecord(pageBuffer, recordOffset, recordLength, sizeof(unsigned)+sizeof(short)-recordLength);
            newFreeBytes = freeBytes-(sizeof(unsigned)+sizeof(short)-recordLength);
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
                                             const RID &rid, const std::string &attributeName, void *data) {
        void* pageBuffer = malloc(PAGE_SIZE);
        short recordOffset, recordLength;
        RID newRid = rid;
        RC errCode = checkAndFindRecord(fileHandle, pageBuffer, recordOffset, recordLength, newRid);
        if (errCode != 0) {
            return errCode;
        }

        int attrOffPtr = recordOffset + sizeof(unsigned);
        const unsigned numAttrs = recordDescriptor.size();
        for (int attrIndex = 0; attrIndex < numAttrs; attrIndex++) {
            Attribute attr = recordDescriptor[attrIndex];
            if (attr.name == attributeName) {
                int attrOffset;
                memcpy(&attrOffset, (char*) pageBuffer+attrOffPtr, sizeof(int));

                // attribute is null
                if (attrOffset == -1) {
                    char nullIndicator = 0;  // 00000000
                    memcpy((char*)data, &nullIndicator, 1);
                }
                // attribute is not null
                else {
                    unsigned char nullIndicator = 128;  // 10000000
                    memcpy((char*)data, &nullIndicator, 1);

                    int attrPtr = recordOffset + sizeof(unsigned) + numAttrs*sizeof(int) + attrOffset;
                    if (attr.type == TypeVarChar) {
                        unsigned varCharLen = 0;
                        memcpy(&varCharLen, (char*) pageBuffer+attrPtr, sizeof(unsigned));
                        memcpy((char*)data, (char*) pageBuffer+attrPtr, sizeof(unsigned)+varCharLen);
                    }
                    else {
                        memcpy((char*)data, (char*) pageBuffer+attrPtr, 4);
                    }
                }
            }
            attrOffPtr += sizeof(int);
        }
        free(pageBuffer);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
       rbfm_ScanIterator.init(fileHandle, recordDescriptor, compOp, value, attributeNames);
       int numAttrs = recordDescriptor.size();
       short attrIdx;
       for (short targetAttrIdx = 0; targetAttrIdx < attributeNames.size(); targetAttrIdx++){
           for (attrIdx = 0; attrIdx < numAttrs; attrIdx++){
               if (recordDescriptor[attrIdx].name == attributeNames[targetAttrIdx]){
                   rbfm_ScanIterator.targetAttrIdx.push_back(attrIdx);
                   break;
               }
           }
           if (attrIdx == numAttrs){
               return -1;   // attributeName not found
           }
       }
       for (attrIdx = 0; attrIdx < numAttrs; attrIdx++){
           if (recordDescriptor[attrIdx].name == conditionAttribute){
               rbfm_ScanIterator.conditionAttrIdx = attrIdx;
               rbfm_ScanIterator.conditionType = recordDescriptor[attrIdx].type;
               break;
           }
       }
       if (attrIdx == numAttrs){
           if (compOp == NO_OP){
               rbfm_ScanIterator.conditionAttrIdx = -1;
           }
           else{
               return -1;
           }
       }

       return 0;
    }

    /*********************************************/
    /*****    functions of scan_Iterator  *******/
    /*********************************************/
    void RBFM_ScanIterator::init(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                               const CompOp compOp, const void *value,
                               const std::vector<std::string> &attributeNames) {
        //this->fileHandle = &fileHandle;
        this->recordDescriptor = recordDescriptor;
        this->compOp = compOp;
        this->value = value;
        this->attributeNames = attributeNames;
        this->currPageNum = 0;
        this->currSlotNum = 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        void *pageBuffer = malloc(PAGE_SIZE);

        //std::fstream file;
        //file.open("Tables");
        //char* buffer = new char[PAGE_SIZE];
        //file.seekg(0,std::ios::beg);
        //file.read(buffer,4*sizeof(unsigned));
        //unsigned numPages = ((unsigned*) buffer)[3];
        //delete[] buffer;

        unsigned numPages = fileHandle.getNumberOfPages();
        int pageNum = 0;
        short slotNum = 0;
        short recordOffset;
        short recordLength;
        bool found = false;
        int numAttrs = recordDescriptor.size();
        short numSlots = -1;
        //std::cout<<"inside getNextRecord before outer for loop, currPageNum is "<<currPageNum<< ", numPages is " << numPages <<std::endl;
        for (pageNum = currPageNum; pageNum < numPages; pageNum++){
            //std::cout<<"inside getNextRecord after enter outer for loop"<<std::endl;
            if (found == true){
                break;
            }
            if (slotNum == numSlots){
                currSlotNum = 0;
            }
            //std::cout<<"inside getNextRecord before readPage"<<std::endl;
            RC errCode = fileHandle.readPage(pageNum, pageBuffer);
            if (errCode != 0) return errCode;
            //std::cout<<"inside getNextRecord before getNumSlots"<<std::endl;
            numSlots = rbfm.getNumSlots(pageBuffer);
            //std::cout<<"inside getNextRecord before inner for loop"<<std::endl;
            for (slotNum = currSlotNum; slotNum < numSlots; slotNum++) {
                std::cout<<"inside getNextRecord slotNum is "<<slotNum<<std::endl;
                std::cout<<"inside getNextRecord numSlots is "<<numSlots<<std::endl;
                recordOffset = rbfm.getRecordOffset(pageBuffer, slotNum);
                recordLength = rbfm.getRecordLength(pageBuffer, slotNum);
                //std::cout<<"inside getNextRecord recordOffset is "<<recordOffset<<std::endl;
                //std::cout<<"inside getNextRecord recordLength is "<<recordLength<<std::endl;
                if (recordOffset == -1 || recordLength == -1){
                    continue;
                }
                if (conditionAttrIdx == -1){
                    found = true;
                    break;
                }
                else{
                    short attrOffset = 0;
                    short attrLen = 0;
                    RC errCode = parseAttr(attrLen, attrOffset, pageBuffer, recordOffset, conditionAttrIdx, numAttrs);
                    if (errCode != 0) return errCode;
                    //std::cout<<"inside getNextRecord conditionAttrIdx is "<<conditionAttrIdx<<std::endl;
                    //std::cout<<"inside getNextRecord attrOffset is "<<attrOffset<<std::endl;
                    //std::cout<<"inside getNextRecord attrlen is "<<attrLen<<std::endl;
                    if (attrLen != 0){
                        void* attrBuffer = malloc(attrLen);
                        memcpy((char*) attrBuffer, (char*) pageBuffer + recordOffset + sizeof(unsigned) + numAttrs*sizeof(int) + attrOffset, attrLen);
                        //std::cout<<"inside getNextRecord before found"<<std::endl;
                        found = findAttr(attrBuffer,attrLen);
                        //std::cout<<"inside getNextRecord after found"<<std::endl;
                        free(attrBuffer);
                        if (found){
                            break;
                        }
                    }
                }
            }
        }
        //std::cout<<"inside getNextRecord after outer for loop" << std::endl;

        if (found){
            rid.pageNum = pageNum-1;
            currPageNum = pageNum-1;
            rid.slotNum = slotNum;
            currSlotNum = slotNum;

            int newNullIndicatorSize = ceil(((double) attributeNames.size())/8);
            //std::cout<<"inside getNextRecord inside if found, newNullIndicatorSize is "<< newNullIndicatorSize << std::endl;
            auto * newNullIndicator = (unsigned char*)malloc(newNullIndicatorSize);
            memset(newNullIndicator,0, newNullIndicatorSize);
            int dataPtr = newNullIndicatorSize;
            //std::cout<<"inside getNextRecord inside if found, dataPtr is "<< dataPtr<<std::endl;


            for (short i = 0; i < attributeNames.size(); i++){
                //std::cout<<"inside getNextRecord inside if found, attrbuteNames.size() is "<< attributeNames.size()<<std::endl;
                //std::cout<<"inside getNextRecord inside if found, i is "<< i<<std::endl;
                short attrLen = 0;
                short attrOffset = 0;
                RC errCode = parseAttr(attrLen, attrOffset, pageBuffer, recordOffset, targetAttrIdx[i], numAttrs);
                if (errCode != 0) return errCode;
                //std::cout<<"inside getNextRecord inside if found, attrOffset is "<< attrOffset<<std::endl;
                //std::cout<<"inside getNextRecord inside if found, attrLen is "<< attrLen<<std::endl;
                if (attrLen > 0){
                    memcpy((char*) data + dataPtr, (char*) pageBuffer + recordOffset + sizeof(unsigned) + numAttrs*sizeof(int) + attrOffset, attrLen);
                    //////// debug
                    int val = 0;
                    //std::cout<<"inside getNextRecord inside if found for loop, dataPtr1 is "<< dataPtr<<std::endl;
                    memcpy(&val, (char*) data + dataPtr, 4);
                    //std::cout << "inside getNextRecord inside if found, value is " << val << std::endl;
                    //////
                    dataPtr += attrLen;
                }
                else{
                    //std::cout << "inside getNextRecord inside if found, NULL value appeared " << std::endl;
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    newNullIndicator[byteIndex] += pow(2, 7-bitIndex);
                }
            }
            //////// debug
            short newNull = newNullIndicatorSize;
            int val = 7;
            memcpy(&val, (char*) data + newNull, sizeof(int));
            //std::cout << "inside getNextRecord after copy nullIndicator, value2 is " << val << std::endl;
            //////
            free(pageBuffer);
            memcpy((char*) data, newNullIndicator, newNullIndicatorSize);
            free(newNullIndicator);
            return 0;
        }
        free(pageBuffer);
        return RBFM_EOF;
    }

    RC RBFM_ScanIterator::parseAttr(short &attrLen, short &attrOffset, void *pageBuffer, short recordOffset, short idx, int numAttrs){
        memcpy(&attrOffset, (char*) pageBuffer + recordOffset + sizeof(unsigned) + idx*sizeof(int), sizeof(int));
        if (attrOffset != -1){
            if (recordDescriptor[idx].type == TypeVarChar){
                unsigned varCharLen = 0;
                memcpy(&varCharLen, (char*) pageBuffer + recordOffset + sizeof(unsigned) + numAttrs*sizeof(int) + attrOffset, sizeof(unsigned));
                attrLen = varCharLen + sizeof(unsigned);
            }
            else{
                attrLen = sizeof(unsigned);
            }
        }
        return 0;
    }
    bool RBFM_ScanIterator::findAttr(const void *checkAttr,short attrLen){
        bool found = false;
        std::cout<<"inside findAttr before if conditionType is "<<conditionType<<std::endl;
        if(conditionType == TypeVarChar) {
            unsigned checkVarCharLen = attrLen - sizeof(unsigned);
            char* checkVarChar = (char*) malloc(checkVarCharLen);
            memcpy(checkVarChar, (char*) checkAttr + sizeof(unsigned), checkVarCharLen);

            unsigned valueVarCharLen;
            memcpy(&valueVarCharLen, (char*) value, sizeof(unsigned));
            std::cout<<"inside findAttr valueVarCharLen is "<<valueVarCharLen<<std::endl;
            char* valueVarChar = (char*) malloc(valueVarCharLen);
            memcpy(valueVarChar, (char*) value + sizeof(unsigned), valueVarCharLen);
            std::cout<<"inside findAttr before switch"<<std::endl;
            switch(compOp){
                case EQ_OP:
                    found = std::string(valueVarChar, valueVarCharLen) == std::string(checkVarChar, checkVarCharLen);
                    std::cout<<"inside switch checkVarChar is "<< std::string((char*) checkAttr +sizeof(unsigned), checkVarCharLen)<<std::endl;
                    std::cout<<"inside switch valueVarChar is "<< std::string((char*) value +sizeof(unsigned), valueVarCharLen)<<std::endl;
                    break;
                case LT_OP:
                    found = std::string(valueVarChar, valueVarCharLen) < std::string(checkVarChar, checkVarCharLen);
                    break;
                case LE_OP:
                    found = std::string(valueVarChar, valueVarCharLen) <= std::string(checkVarChar, checkVarCharLen);
                    break;
                case GT_OP:
                    found = std::string(valueVarChar, valueVarCharLen) > std::string(checkVarChar, checkVarCharLen);
                    break;
                case GE_OP:
                    found = std::string(valueVarChar, valueVarCharLen) >= std::string(checkVarChar, checkVarCharLen);
                    break;
                case NE_OP:
                    found = std::string(valueVarChar, valueVarCharLen) != std::string(checkVarChar, checkVarCharLen);
                    break;
            }
            free(checkVarChar);
            free(valueVarChar);
        }
        else if (conditionType == TypeInt){
            std::cout<<"TypeInt"<<std::endl;
            int check;
            memcpy(&check, (char*)checkAttr, sizeof(int));
            int data;
            memcpy(&data, (char*)value, sizeof(int));
            switch(compOp){
                case EQ_OP:
                    found = data == check;
                    std::cout<<"inside switch data is "<< data <<std::endl;
                    std::cout<<"inside switch check is "<< check <<std::endl;
                    break;
                case LT_OP:
                    found = data < check;
                    break;
                case LE_OP:
                    found = data <= check;
                    break;
                case GT_OP:
                    found = data > check;
                    break;
                case GE_OP:
                    found = data >= check;
                    break;
                case NE_OP:
                    found = data != check;
                    break;
            }
        }
        else if (conditionType == TypeReal){
            std::cout<<"TypeReal"<<std::endl;
            float check;
            memcpy(&check, (char*)checkAttr, sizeof(int));
            float data;
            memcpy(&data, (char*)value, sizeof(int));
            switch(compOp){
                case EQ_OP:
                    found = data == check;
                    break;
                case LT_OP:
                    found = data < check;
                    break;
                case LE_OP:
                    found = data <= check;
                    break;
                case GT_OP:
                    found = data > check;
                    break;
                case GE_OP:
                    found = data >= check;
                    break;
                case NE_OP:
                    found = data != check;
                    break;
            }
        }
        return found;
    }

    RC RBFM_ScanIterator::close(){
        //currPageNum = 0;
        //currSlotNum = 0;
        targetAttrIdx.clear();
        return fileHandle.closeFile();
    }
    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    short RecordBasedFileManager::generateRecordLength(const std::vector<Attribute> &recordDescriptor, const void *data) {
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
                        //std::cout <<"inside generateRecord varCharLen is "<< varCharLen <<std::endl;
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
                    int nullAttrOffset = -1;
                    memcpy(offsetDirPtr, &nullAttrOffset, sizeof(int));
                    offsetDirPtr += 1;
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
        setNumSlots(pageBuffer, numSlots);
        setFreeBytes(pageBuffer, freeBytes);
    }

    bool RecordBasedFileManager::insertRecordToPage(void* recordBuffer, short& recordOffset, short recordLength, void* pageBuffer) {
        recordOffset = getInsertStartOffset(pageBuffer);
        //std::cout << "Inside insertRecordToPage: insertStartOffset is " << recordOffset << std::endl;
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
        unsigned short newFreeBytes = getFreeBytes(pageBuffer)-2*sizeof(short);
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

                // Accumulate the length of  records to be shifted
                currRecordLength = getRecordLength(pageBuffer, slotNum);
                sizeToBeShifted += currRecordLength;
            }
        }
        memcpy((char*) pageBuffer+recordOffset+recordLength+distance, (char*) pageBuffer+recordOffset+recordLength, sizeToBeShifted);
    }

    RC RecordBasedFileManager::checkAndFindRecord(FileHandle &fileHandle, void *pageBuffer,
                                                  short &recordOffset, short &recordLength, RID &rid) {
        unsigned numPages = fileHandle.getNumberOfPages();
        std::cout<<"inside checkAnd FindRecord rid.pageNum is "<<rid.pageNum<<std::endl;
        std::cout<<"inside checkAnd FindRecord numPages is "<<numPages<<std::endl;
        // pageNum is not valid
        if (rid.pageNum >= numPages){
            return -1;
        }

        fileHandle.readPage(rid.pageNum, pageBuffer);

        // slotNum is not valid
        unsigned short numSlots = getNumSlots(pageBuffer);
        std::cout<<"inside checkAnd FindRecord rid.slotNum is "<<rid.slotNum<<std::endl;
        std::cout<<"inside checkAnd FindRecord numSlots is "<<numSlots<<std::endl;
        if (rid.slotNum >= numSlots){
            //free(pageBuffer);
            return -1;
        }

        return findRecord(fileHandle, pageBuffer,recordOffset, recordLength, rid);
    }

    RC RecordBasedFileManager::findRecord(FileHandle &fileHandle, void *pageBuffer,
                                            short &recordOffset, short &recordLength, RID &rid) {
        // get record offset. If is -1, record is already deleted.
        recordOffset = getRecordOffset(pageBuffer, rid.slotNum);
        if (recordOffset == -1){
            return -1;
        }

        // get record length. If is -1, record is on another page
        recordLength = getRecordLength(pageBuffer, rid.slotNum);

        unsigned newPageNum = rid.pageNum;
        unsigned short newSlotNum = rid.slotNum;
        while (recordLength == -1) {
            memcpy(&newPageNum, (char*) pageBuffer + recordOffset, sizeof(unsigned));
            memcpy(&newSlotNum, (char*) pageBuffer + recordOffset + sizeof(unsigned), sizeof(short));

            fileHandle.readPage(newPageNum, pageBuffer);

            // get record offset. If is -1, record is already deleted.
            recordOffset = getRecordOffset(pageBuffer, newSlotNum);
            if (recordOffset == -1){
                return -1;
            }
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
        memcpy(&numSlots, (char*) pageBuffer+PAGE_SIZE-2*sizeof(short), sizeof(short));
        return numSlots;
    }

    unsigned short RecordBasedFileManager::getFreeBytes(void* pageBuffer) {
        unsigned short freeBytes;
        memcpy(&freeBytes, (char*) pageBuffer+PAGE_SIZE-sizeof(short), sizeof(short));
        return freeBytes;
    }

    short RecordBasedFileManager::getRecordLength(void* pageBuffer, unsigned short slotNum) {
        short recordLength;
        memcpy(&recordLength, (char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), sizeof(short));
        return recordLength;
    }

    short RecordBasedFileManager::getRecordOffset(void* pageBuffer, unsigned short slotNum) {
        short recordOffset;
        memcpy(&recordOffset, (char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), sizeof(short));
        return recordOffset;
    }

    void RecordBasedFileManager::setNumSlots(void* pageBuffer, unsigned short numSlots) {
        memcpy((char*) pageBuffer+PAGE_SIZE-2*sizeof(short), &numSlots, sizeof(short));
    }

    void RecordBasedFileManager::setFreeBytes(void* pageBuffer, unsigned short freeBytes) {
        memcpy((char*) pageBuffer+PAGE_SIZE-sizeof(short), &freeBytes, sizeof(short));
    }

    void RecordBasedFileManager::setRecordLength(void* pageBuffer, unsigned short slotNum, short recordLength) {
        memcpy((char*) pageBuffer+PAGE_SIZE-(3+2*slotNum)*sizeof(short), &recordLength, sizeof(short));
    }

    void RecordBasedFileManager::setRecordOffset(void* pageBuffer, unsigned short slotNum, short recordOffset) {
        memcpy((char*) pageBuffer+PAGE_SIZE-(4+2*slotNum)*sizeof(short), &recordOffset, sizeof(short));
    }

} // namespace PeterDB

