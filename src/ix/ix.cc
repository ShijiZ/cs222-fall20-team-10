#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    IndexManager::IndexManager() {
        PagedFileManager& pfm = PagedFileManager::instance();
        this->pfm = &pfm;
    }

    IndexManager::~IndexManager() = default;

    RC IndexManager::createFile(const std::string &fileName) {
        return pfm->createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return pfm->destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return pfm->openFile(fileName, ixFileHandle.fileHandle);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return pfm->closeFile(ixFileHandle.fileHandle);
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        void* pageBuffer = malloc(PAGE_SIZE);
        AttrType attrType = attribute.type;
        unsigned short keyLength = getKeyLength(key, attrType);

        // B+ tree has 0 node
        if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
            // set up meta data page
            unsigned rootPageNum = 1;
            memcpy((char*) pageBuffer, &rootPageNum, ROOT_PAGE_NUM_SIZE);

            // append meta data page
            RC errCode = ixFileHandle.fileHandle.appendPage(pageBuffer);
            if (errCode != 0) return errCode;

            // set up the first leaf node (root)
            unsigned short bytesNeeded = keyLength + PTR_PN_SIZE + PTR_SN_SIZE;
            void* firstEntry = malloc(bytesNeeded);
            memcpy((char*) firstEntry, (char*) key, keyLength);
            memcpy((char*) firstEntry+keyLength, &rid.pageNum, PTR_PN_SIZE);
            memcpy((char*) firstEntry+keyLength+PTR_PN_SIZE, &rid.slotNum, PTR_SN_SIZE);

            errCode = initLeafNode(pageBuffer, firstEntry, bytesNeeded, 1, -1);
            if (errCode != 0) return errCode;
            free(firstEntry);

            // append the first leaf node (root)
            errCode = ixFileHandle.fileHandle.appendPage(pageBuffer);
            if (errCode != 0) return errCode;
        }
        // B+ tree has at least 1 node
        else {
            // get root page
            unsigned rootPageNum = getRootPageNum(ixFileHandle);
            RC errCode = ixFileHandle.fileHandle.readPage(rootPageNum, pageBuffer);
            if (errCode != 0) return errCode;

            // allocate newChildEntry, insert entry to root recursively
            void* newChildEntry = malloc(PAGE_SIZE);
            errCode = insertEntryRec(ixFileHandle, pageBuffer, rootPageNum, keyLength,
                                   attrType, key, rid, newChildEntry, rootPageNum);
            if (errCode != 0) return errCode;
            free(newChildEntry);
        }
        free(pageBuffer);
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        // B+ tree has 0 node, return error
        if (ixFileHandle.fileHandle.getNumberOfPages() == 0) return -1;

        void* pageBuffer = malloc(PAGE_SIZE);
        AttrType attrType = attribute.type;
        unsigned short keyLength = getKeyLength(key, attrType);

        // get root page
        unsigned pageNumTobeDeleted = getRootPageNum(ixFileHandle);
        RC errCode = ixFileHandle.fileHandle.readPage(pageNumTobeDeleted, pageBuffer);
        if (errCode != 0) return errCode;

        // find the leaf node (potentially) containing the entry
        bool isLeaf = getIsLeaf(pageBuffer);
        unsigned short numKeys = getNumKeys(pageBuffer);
        while (!isLeaf) {
            errCode = findPageNumToBeHandled(pageBuffer, keyLength, key, rid,
                                             numKeys, attrType, pageNumTobeDeleted);
            if (errCode != 0) return errCode;

            errCode = ixFileHandle.fileHandle.readPage(pageNumTobeDeleted, pageBuffer);
            if (errCode != 0) return errCode;

            isLeaf = getIsLeaf(pageBuffer);
            numKeys = getNumKeys(pageBuffer);
        }
        //std::cout << "inside deleteEntry, pageNumTobeDeleted is "<<pageNumTobeDeleted<<std::endl;

        unsigned short currKeyPtr = IS_LEAF_SIZE;
        unsigned currPageNum;
        unsigned short currSlotNum;
        unsigned short sizePassed;
        int keyIdx;
        // attribute type is varChar
        if (attrType == TypeVarChar) {
            // generate key varChar
            std::string keyVarChar = std::string((char*) key+VC_LEN_SIZE, keyLength-VC_LEN_SIZE);

            unsigned currVarCharLen;
            for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                // generate current varChar
                memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);

                // break loop if key varChar found
                if (keyVarChar == currKeyVarChar) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);
                        if (rid.slotNum == currSlotNum) break;
                    }
                }

                // not found, go to next varChar
                sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                currKeyPtr += sizePassed;
            }
        }
        // attribute type is int
        else if (attrType == TypeInt) {
            // generate key int
            int keyInt;
            memcpy(&keyInt, (char*) key, INT_SIZE);

            int currKeyInt;
            for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                // generate current int
                memcpy(&currKeyInt, (char*) pageBuffer+currKeyPtr, INT_SIZE);

                // break loop if key int found
                if (keyInt == currKeyInt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        if (rid.slotNum == currSlotNum) break;
                    }
                }

                // not found, go to next int
                sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                currKeyPtr += sizePassed;
            }
        }
        // attribute type is float
        else {
            // generate key float
            float keyFlt;
            memcpy(&keyFlt, (char*) key, FLT_SIZE);

            float currKeyFlt;
            for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                // generate current float
                memcpy(&currKeyFlt, (char*) pageBuffer+currKeyPtr, FLT_SIZE);

                // break loop if key float found
                if (keyFlt == currKeyFlt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        if (rid.slotNum == currSlotNum) break;
                    }
                }

                // not found, go to next float
                sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                currKeyPtr += sizePassed;
            }
        }

        // if key didn't found in the entire page, key doesn't exist
        if (keyIdx == numKeys) {
            free(pageBuffer);
            return -1;
        }
        // std::cout << "Inside deleteEntry, keyIdx is " << keyIdx << ", numKeys is " << numKeys << std::endl;

        // shift all keys after key to be deleted to the left, update numKeys and freeBytes
        memcpy((char*) pageBuffer+currKeyPtr,
               (char*) pageBuffer+currKeyPtr+keyLength+PTR_PN_SIZE+PTR_SN_SIZE,
               PAGE_SIZE-NXT_PN_SIZE-N_SIZE-F_SIZE-currKeyPtr-keyLength-PTR_PN_SIZE-PTR_SN_SIZE);
        setNumKeys(pageBuffer, numKeys-1);
        setFreeBytes(pageBuffer, getFreeBytes(pageBuffer)+keyLength+PTR_PN_SIZE+PTR_SN_SIZE);
        //std::cout << "inside deleteEntry, freeBytes is"<<getFreeBytes(pageBuffer)<<std::endl;
        ixFileHandle.fileHandle.writePage(pageNumTobeDeleted, pageBuffer);

        free(pageBuffer);
        return 0;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return ix_ScanIterator.initialize(ixFileHandle, attribute, lowKey, highKey,
                                          lowKeyInclusive, highKeyInclusive, this);
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        unsigned rootPageNum = getRootPageNum(ixFileHandle);
        return printNode(ixFileHandle, attribute.type, rootPageNum, 0, out);
    }

    /*********************************************/
    /*****    Getter and Setter functions  *******/
    /*********************************************/
    unsigned short IndexManager::getKeyLength(const void *key, AttrType attrType) {
        unsigned short keyLength;
        if (attrType == TypeVarChar) {
            memcpy(&keyLength, key, VC_LEN_SIZE);
            keyLength += VC_LEN_SIZE;
        }
        else {
            keyLength = INT_OR_FLT_SIZE;
        }
        return keyLength;
    }

    unsigned IndexManager::getRootPageNum(IXFileHandle &ixFileHandle) const {
        // get meta data page
        void* pageBuffer = malloc(PAGE_SIZE);
        RC errCode = ixFileHandle.fileHandle.readPage(0, pageBuffer);
        if (errCode != 0) return errCode;

        // get root page num
        unsigned rootPageNum;
        memcpy(&rootPageNum, (char*) pageBuffer, ROOT_PAGE_NUM_SIZE);
        free(pageBuffer);
        return rootPageNum;
    }

    bool IndexManager::getIsLeaf(void* pageBuffer) const {
        bool isLeaf;
        memcpy(&isLeaf, (char*) pageBuffer, IS_LEAF_SIZE);
        return isLeaf;
    }

    unsigned short IndexManager::getNumKeys(void* pageBuffer) const {
        unsigned short numKeys;
        memcpy(&numKeys, (char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE, N_SIZE);
        return numKeys;
    }

    unsigned short IndexManager::getFreeBytes(void* pageBuffer) const {
        unsigned short freeBytes;
        memcpy(&freeBytes, (char*) pageBuffer+PAGE_SIZE-F_SIZE, F_SIZE);
        return freeBytes;
    }

    int IndexManager::getNextPageNum(void* pageBuffer) const {
        int nextPageNum;
        memcpy(&nextPageNum, (char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE-NXT_PN_SIZE, NXT_PN_SIZE);
        return nextPageNum;
    }

    void IndexManager::setIsLeaf(void* pageBuffer, bool isLeaf) {
        memcpy((char*) pageBuffer, &isLeaf, IS_LEAF_SIZE);
    }

    void IndexManager::setNumKeys(void* pageBuffer, unsigned short numKeys) {
        memcpy((char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE, &numKeys, N_SIZE);
    }

    void IndexManager::setFreeBytes(void* pageBuffer, unsigned short freeBytes) {
        memcpy((char*) pageBuffer+PAGE_SIZE-F_SIZE, &freeBytes, F_SIZE);
    }

    void IndexManager::setNextPageNum(void* pageBuffer, int nextPageNum) {
        memcpy((char*) pageBuffer+PAGE_SIZE-N_SIZE-F_SIZE-NXT_PN_SIZE, &nextPageNum, NXT_PN_SIZE);
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    RC IndexManager::insertEntryRec(IXFileHandle &ixFileHandle, void* pageBuffer, unsigned pageNum,
                                  unsigned keyLength, AttrType attrType, const void *key,
                                  const RID &rid, void* &newChildEntry, unsigned rootPageNum) {
        bool isRoot = pageNum == rootPageNum;
        bool isLeaf = getIsLeaf(pageBuffer);
        unsigned short freeBytes = getFreeBytes(pageBuffer);
        unsigned short numKeys = getNumKeys(pageBuffer);
        //std::cout << "Inside insertEntry1, isLeaf? " << isLeaf << std::endl;

        // node is non-leaf node
        if (isLeaf) {
            // generate the entry to be inserted to leaf node
            unsigned short bytesNeeded = keyLength + PTR_PN_SIZE + PTR_SN_SIZE;
            void* entryToBeInserted = malloc(bytesNeeded);
            memcpy((char*) entryToBeInserted, (char*) key, keyLength);
            memcpy((char*) entryToBeInserted + keyLength, &rid.pageNum, PTR_PN_SIZE);
            memcpy((char*) entryToBeInserted + keyLength + PTR_PN_SIZE, &rid.slotNum, PTR_SN_SIZE);

            // new key can be inserted in this leaf node
            if (bytesNeeded <= freeBytes) {
                RC errCode = insertEntryToPage(pageBuffer, entryToBeInserted, bytesNeeded,
                                               freeBytes, numKeys, attrType, true, false);
                if(errCode != 0) return errCode;
                newChildEntry = nullptr;

                return ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
            }
            // new key cannot be inserted in this leaf node, must split
            else {
                RC errCode = splitNode(ixFileHandle, pageBuffer, entryToBeInserted, bytesNeeded,newChildEntry,
                                       pageNum, freeBytes, numKeys, attrType, true, isRoot);
                if(errCode != 0) return errCode;
            }
            free(entryToBeInserted);
        }
        // node is non-leaf node
        else {
            unsigned pageNumToBeInserted;
            RC errCode = findPageNumToBeHandled(pageBuffer, keyLength, key, rid,
                                                numKeys, attrType, pageNumToBeInserted);
            // std::cout<< "inside insertEntry1, after findPageNumToBeHandled pageNumToBeInserted is " << pageNumToBeInserted << std::endl;
            if(errCode != 0) return errCode;

            errCode = ixFileHandle.fileHandle.readPage(pageNumToBeInserted, pageBuffer);
            if(errCode != 0) return errCode;

            errCode = insertEntryRec(ixFileHandle, pageBuffer, pageNumToBeInserted,
                                     keyLength, attrType, key, rid, newChildEntry, rootPageNum);
            if(errCode != 0) return errCode;
            //std::cout<< "inside insertEntry1, after findPageNumToBeHandled newChildEntry == nullptr is " << (newChildEntry == nullptr) << std::endl;

            // no split in child nodes
            if (newChildEntry == nullptr) return 0;
            // split happens in child nodes, need to insert copied/pushed up newChildEntry into this non-leaf node
            else {
                errCode = ixFileHandle.fileHandle.readPage(pageNum, pageBuffer);
                if(errCode != 0) return errCode;

                unsigned short bytesNeeded = getKeyLength(newChildEntry, attrType)+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                // newChildEntry can be inserted in this non-leaf node
                if (bytesNeeded <= freeBytes) {
                    RC errCode = insertEntryToPage(pageBuffer, newChildEntry, bytesNeeded,
                                                   freeBytes, numKeys, attrType, false, false);
                    if(errCode != 0) return errCode;
                    newChildEntry = nullptr;

                    //std::cout << "Inside insertEntry1, child split, newChildEntry inserted to parent，numKeys is " << numKeys+1 << std::endl;
                    return ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
                }
                // newChildEntry cannot be inserted in this non-leaf node, must split
                else {
                    RC errCode = splitNode(ixFileHandle, pageBuffer, newChildEntry, bytesNeeded,newChildEntry,
                                           pageNum, freeBytes, numKeys, attrType, false, isRoot);
                    if(errCode != 0) return errCode;
                }
            }
        }
        return 0;
    }

    RC IndexManager::insertEntryToPage(void* pageBuffer, const void *key, unsigned short bytesNeeded,
                                       unsigned short freeBytes, unsigned short numKeys,
                                       AttrType attrType, bool isLeaf, bool isHuge) {
        // initialize currKeyPtr, sizeToBeShifted and keyLength
        unsigned short currKeyPtr;
        unsigned short sizeToBeShifted;
        unsigned short keyLength;
        if (isLeaf) {
            currKeyPtr = IS_LEAF_SIZE;
            sizeToBeShifted = PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE-freeBytes-IS_LEAF_SIZE;
            keyLength = bytesNeeded-PTR_PN_SIZE-PTR_SN_SIZE;
        }
        else {
            currKeyPtr = IS_LEAF_SIZE + PTR_PN_SIZE;
            sizeToBeShifted = PAGE_SIZE-F_SIZE-N_SIZE-freeBytes-PTR_PN_SIZE-IS_LEAF_SIZE;
            keyLength = bytesNeeded-PTR_PN_SIZE-PTR_SN_SIZE-PTR_PN_SIZE;
        }

        // initialize rid
        RID rid;
        memcpy(&rid.pageNum, (char*) key+keyLength, PTR_PN_SIZE);
        memcpy(&rid.slotNum, (char*) key+keyLength+PTR_PN_SIZE, PTR_SN_SIZE);

        unsigned currPageNum;
        unsigned short currSlotNum;
        unsigned short sizePassed;
        // attribute type is varChar
        if (attrType == TypeVarChar) {
            // generate key varChar
            std::string newKeyVarChar = std::string((char*) key+VC_LEN_SIZE, keyLength-VC_LEN_SIZE);

            unsigned currVarCharLen;
            for (int i = 0; i < numKeys; i++) {
                // generate current varChar
                memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);

                // break loop if key varChar inserting point found
                if (newKeyVarChar == currKeyVarChar) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyVarChar < currKeyVarChar) break;

                // if not, go to next varChar
                if (isLeaf) sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                sizeToBeShifted -= sizePassed;
                currKeyPtr += sizePassed;
            }
        }
        // attribute type is int
        else if (attrType == TypeInt) {
            // generate key int
            int newKeyInt;
            memcpy(&newKeyInt, (char*) key, INT_SIZE);

            int currKeyInt;
            for (int i = 0; i < numKeys; i++) {
                // generate current int
                memcpy(&currKeyInt, (char*) pageBuffer+currKeyPtr, INT_SIZE);

                // break loop if key int inserting point found
                if (newKeyInt == currKeyInt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyInt < currKeyInt) break;

                // if not, go to next int
                if (isLeaf) sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                sizeToBeShifted -= sizePassed;
                currKeyPtr += sizePassed;
            }
        }
        // attribute type is float
        else {
            // generate key float
            float newKeyFlt;
            memcpy(&newKeyFlt, (char*) key, FLT_SIZE);

            float currKeyFlt;
            for (int i = 0; i < numKeys; i++) {
                // generate current float
                memcpy(&currKeyFlt, (char*) pageBuffer+currKeyPtr, FLT_SIZE);

                // break loop if key float inserting point found
                if (newKeyFlt == currKeyFlt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyFlt < currKeyFlt) break;

                // if not, go to next float
                if (isLeaf) sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                sizeToBeShifted -= sizePassed;
                currKeyPtr += sizePassed;
            }
        }
        // shift all keys after key to be inserted to the right,
        memcpy((char*) pageBuffer+currKeyPtr+bytesNeeded, (char*) pageBuffer+currKeyPtr, sizeToBeShifted);
        memcpy((char*) pageBuffer+currKeyPtr, (char*) key, bytesNeeded);

        // update numKeys and freeBytes if this page is not a huge page
        if (!isHuge) {
            setFreeBytes(pageBuffer, freeBytes-bytesNeeded);
            setNumKeys(pageBuffer, numKeys+1);
        }
        return 0;
    }

    RC IndexManager::findPageNumToBeHandled(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                                             unsigned short numKeys, AttrType attrType, unsigned& pageNumToBeHandled) {
        unsigned short currKeyPtr = IS_LEAF_SIZE + PTR_PN_SIZE;
        unsigned currPageNum;
        unsigned short currSlotNum;
        // attribute type is varChar
        if (attrType == TypeVarChar) {
            // generate key varChar
            std::string newKeyVarChar = std::string((char*) key+VC_LEN_SIZE, keyLength-VC_LEN_SIZE);

            unsigned currVarCharLen;
            for (int i = 0; i < numKeys; i++) {
                // generate current varChar
                memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);

                // break loop if page need to be handled found
                if (newKeyVarChar == currKeyVarChar) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyVarChar < currKeyVarChar) break;

                // if not, go to next varChar
                unsigned short sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                currKeyPtr += sizePassed;
            }
        }
        // attribute type is int
        else if (attrType == TypeInt) {
            // generate key int
            int newKeyInt;
            memcpy(&newKeyInt, (char*) key, INT_SIZE);

            int currKeyInt;
            for (int i = 0; i < numKeys; i++) {
                // generate current int
                memcpy(&currKeyInt, (char*) pageBuffer+currKeyPtr, INT_SIZE);

                // break loop if page need to be handled found
                if (newKeyInt == currKeyInt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyInt < currKeyInt) break;

                // if not, go to next int
                unsigned short sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                currKeyPtr += sizePassed;
            }
        }
        // attribute type is float
        else {
            // generate key float
            float newKeyFlt;
            memcpy(&newKeyFlt, (char*) key, FLT_SIZE);

            float currKeyFlt;
            for (int i = 0; i < numKeys; i++) {
                // generate current float
                memcpy(&currKeyFlt, (char*) pageBuffer+currKeyPtr, FLT_SIZE);

                // break loop if page need to be handled found
                if (newKeyFlt == currKeyFlt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyFlt < currKeyFlt) break;

                // if not, go to next float
                unsigned short sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                currKeyPtr += sizePassed;
            }
        }

        // pass out page num to be handled
        memcpy(&pageNumToBeHandled, (char*) pageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);
        return 0;
    }

    RC IndexManager::splitNode(IXFileHandle &ixFileHandle, void* pageBuffer, const void *key, unsigned short bytesNeeded,
                               void* &newChildEntry, unsigned pageNum, unsigned short freeBytes, unsigned short numKeys,
                               AttrType attrType, bool isLeaf, bool isRoot) {
        // initialize huge page buffer
        void* hugePageBuffer = malloc(2*PAGE_SIZE);
        memcpy(hugePageBuffer, pageBuffer, PAGE_SIZE);

        // insert new key into huge page buffer
        RC errCode = insertEntryToPage(hugePageBuffer, key, bytesNeeded, freeBytes,
                                       numKeys, attrType, isLeaf, true);
        if(errCode != 0) return errCode;

        // initialize currKeyPtr and sizeToBeCopied (to right sibling)
        unsigned short currKeyPtr;
        unsigned short sizeToBeCopied;
        if (isLeaf) {
            currKeyPtr = IS_LEAF_SIZE;
            sizeToBeCopied = PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE-freeBytes-IS_LEAF_SIZE+bytesNeeded;
        }
        else {
            currKeyPtr = IS_LEAF_SIZE+PTR_PN_SIZE;
            sizeToBeCopied = PAGE_SIZE-F_SIZE-N_SIZE-freeBytes-IS_LEAF_SIZE-PTR_PN_SIZE+bytesNeeded;
        }

        unsigned short sizeToBePassed;
        int keyIdx;
        // attribute type is varChar
        if (attrType == TypeVarChar) {
            unsigned currVarCharLen;
            for (keyIdx = 0; keyIdx < numKeys+1; keyIdx++) {
                // if sizeToBeCopied is less than order, prepare left sibling (original pageBuffer) and newChildEntry, break
                if (sizeToBeCopied < ORDER) {
                    // copy left half contents of huge page buffer to left sibling (original pageBuffer)
                    memcpy((char*) pageBuffer, (char*) hugePageBuffer, PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE);

                    // set numKeys and freeBytes of left sibling (original pageBuffer)
                    setNumKeys(pageBuffer, keyIdx-1);
                    unsigned short leftSiblingFreeBytes;
                    if (isLeaf) leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr+sizeToBePassed-F_SIZE-N_SIZE-NXT_PN_SIZE;
                    else leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr+sizeToBePassed-F_SIZE-N_SIZE;
                    setFreeBytes(pageBuffer, leftSiblingFreeBytes);

                    // prepare newChildEntry (without pointer to right sibling, see below) as middle entry
                    memcpy((char*) newChildEntry, (char*) hugePageBuffer+currKeyPtr-sizeToBePassed, sizeToBePassed);
                    break;
                }

                // if not, go to next varChar
                memcpy(&currVarCharLen, (char*) hugePageBuffer+currKeyPtr, VC_LEN_SIZE);
                if (isLeaf) sizeToBePassed = VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizeToBePassed = VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                sizeToBeCopied -= sizeToBePassed;
                currKeyPtr += sizeToBePassed;
            }
        }
        // attribute type is int or float
        else {
            if (isLeaf) sizeToBePassed = INT_OR_FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
            else sizeToBePassed = INT_OR_FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;

            for (keyIdx = 0; keyIdx < numKeys+1; keyIdx++) {
                // if sizeToBeCopied is less than order, prepare left sibling (original pageBuffer) and newChildEntry, break
                if (sizeToBeCopied < ORDER) {
                    // copy left half contents of huge page buffer to left sibling (original pageBuffer)
                    memcpy((char*) pageBuffer, (char*) hugePageBuffer, PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE);

                    // set numKeys and freeBytes of left sibling (original pageBuffer)
                    setNumKeys(pageBuffer, keyIdx-1);
                    unsigned short leftSiblingFreeBytes;
                    if (isLeaf) leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr+sizeToBePassed-F_SIZE-N_SIZE-NXT_PN_SIZE;
                    else leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr+sizeToBePassed-F_SIZE-N_SIZE;
                    setFreeBytes(pageBuffer, leftSiblingFreeBytes);

                    // prepare newChildEntry (without pointer to right sibling, see below) as middle entry
                    memcpy((char*) newChildEntry, (char*) hugePageBuffer+currKeyPtr-sizeToBePassed, sizeToBePassed);
                    break;
                }

                // if not, go to next int or float
                sizeToBeCopied -= sizeToBePassed;
                currKeyPtr += sizeToBePassed;
            }
        }

        // prepare right sibling
        int leftSiblingNxtPageNum = getNextPageNum(pageBuffer);
        void* rightSiblingPageBuffer = malloc(PAGE_SIZE);
        if (isLeaf) { // copy up
            errCode = initLeafNode(rightSiblingPageBuffer, (char*) hugePageBuffer+currKeyPtr-sizeToBePassed,
                                   sizeToBeCopied+sizeToBePassed, numKeys+2-keyIdx, leftSiblingNxtPageNum);
        }
        else { // push up
            errCode = initNonLeafNode(rightSiblingPageBuffer, (char*) hugePageBuffer+currKeyPtr-PTR_PN_SIZE,
                                      sizeToBeCopied+PTR_PN_SIZE, numKeys+1-keyIdx);
        }
        if(errCode != 0) return errCode;
        free(hugePageBuffer);

        // append right sibling
        errCode = ixFileHandle.fileHandle.appendPage(rightSiblingPageBuffer);
        if(errCode != 0) return errCode;
        free(rightSiblingPageBuffer);

        // finish prepare left sibling and newChildEntry
        int rightSiblingPageNum = ixFileHandle.fileHandle.getNumberOfPages()-1;
        if (isLeaf) {
            memcpy((char*) newChildEntry+sizeToBePassed, &rightSiblingPageNum, PTR_PN_SIZE);
            setNextPageNum(pageBuffer, rightSiblingPageNum);
        }
        else{
            memcpy((char*) newChildEntry+sizeToBePassed-PTR_PN_SIZE, &rightSiblingPageNum, PTR_PN_SIZE);
        }

        // write left sibling
        errCode = ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
        if(errCode != 0) return errCode;

        // need to create new root
        if (isRoot) {
            void* newRootPageBuffer = malloc(PAGE_SIZE);

            // new root entry contains ptr to left sibling, varChar/int/float, rid, ptr to right sibling
            unsigned short newRootEntryLength;
            if (isLeaf) newRootEntryLength = sizeToBePassed + 2*PTR_PN_SIZE;
            else newRootEntryLength = sizeToBePassed + PTR_PN_SIZE;

            // prepare new root entry: 1. set ptr to left sibling, 2. set newChildEntry
            void* newRootEntry = malloc(newRootEntryLength);
            memcpy((char*) newRootEntry, &pageNum, PTR_PN_SIZE);
            memcpy((char*) newRootEntry+PTR_PN_SIZE, (char*) newChildEntry, newRootEntryLength-PTR_PN_SIZE);

            errCode = initNonLeafNode(newRootPageBuffer, newRootEntry, newRootEntryLength, 1);
            if(errCode != 0) return errCode;

            // append new root node
            errCode = ixFileHandle.fileHandle.appendPage(newRootPageBuffer);
            if(errCode != 0) return errCode;

            // update meta data page
            unsigned newRootPageNum = ixFileHandle.fileHandle.getNumberOfPages()-1;
            errCode = ixFileHandle.fileHandle.readPage(0, pageBuffer);
            if (errCode != 0) return errCode;

            memcpy((char*) pageBuffer, &newRootPageNum, sizeof(unsigned));
            errCode = ixFileHandle.fileHandle.writePage(0, pageBuffer);
            if (errCode != 0) return errCode;

            free(newRootPageBuffer);
            free(newRootEntry);
        }

        return 0;
    }

    RC IndexManager::initLeafNode(void* pageBuffer, void* entryPtr, unsigned short bytesNeeded,
                                  int numKeys, int nextPageNum){
        bool isLeaf = true;
        setIsLeaf(pageBuffer, isLeaf);
        memcpy((char*) pageBuffer + IS_LEAF_SIZE, entryPtr, bytesNeeded);
        setNextPageNum(pageBuffer, nextPageNum);
        setNumKeys(pageBuffer,numKeys);
        setFreeBytes(pageBuffer, PAGE_SIZE - IS_LEAF_SIZE - N_SIZE - F_SIZE - NXT_PN_SIZE - bytesNeeded);
        return 0;
    }

    RC IndexManager::initNonLeafNode(void* pageBuffer, void* entryPtr, unsigned short bytesNeeded, int numKeys){
        bool isLeaf = false;
        setIsLeaf(pageBuffer, isLeaf);
        memcpy((char*) pageBuffer + IS_LEAF_SIZE, entryPtr, bytesNeeded);
        setNumKeys(pageBuffer,numKeys);
        setFreeBytes(pageBuffer, PAGE_SIZE - IS_LEAF_SIZE - N_SIZE - F_SIZE - bytesNeeded);
        return 0;
    }

    RC IndexManager::printNode(IXFileHandle &ixFileHandle, AttrType attrType,
                               unsigned pageNum, int indent, std::ostream &out) const {
        void* pageBuffer = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum, pageBuffer);

        bool isLeaf = getIsLeaf(pageBuffer);
        unsigned short numKeys = getNumKeys(pageBuffer);

        for (int i = 0; i < indent; i++) out << " ";

        if (isLeaf) {
            unsigned short currKeyPtr = IS_LEAF_SIZE;
            unsigned currPageNum;
            unsigned short currSlotNum;
            out << "{\"keys\": [";
            // attribute type is varChar
            if (attrType == TypeVarChar) {
                unsigned currVarCharLen;
                std::string currKeyVarChar;
                std::string prevKeyVarChar = "";
                for (int i = 0; i < numKeys; i++) {
                    // generate current varChar and rid
                    memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                    currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                    memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);

                    // print current varChar and rid
                    if (i != 0 && currKeyVarChar != prevKeyVarChar) out << "]\",";
                    if (i == 0 || currKeyVarChar != prevKeyVarChar) {
                        out << "\"" << currKeyVarChar << ":[(" << currPageNum << "," << currSlotNum << ")";
                    }
                    else out << ",(" << currPageNum << "," << currSlotNum << ")";
                    if (i == numKeys-1) out << "]\"";

                    // go to next varChar
                    unsigned short sizePassed = currVarCharLen + VC_LEN_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                    prevKeyVarChar = currKeyVarChar;
                }
            }
            // attribute type is int
            else if (attrType == TypeInt) {
                int currKeyInt;
                int prevKeyInt = 0;
                for (int i = 0; i < numKeys; i++) {
                    // generate current int and rid
                    memcpy(&currKeyInt, (char*) pageBuffer + currKeyPtr, INT_SIZE);
                    memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr + INT_SIZE, PTR_PN_SIZE);
                    memcpy(&currSlotNum, (char*) pageBuffer + currKeyPtr + INT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);

                    // print current int and rid
                    if (i != 0 && currKeyInt != prevKeyInt) out << "]\",";
                    if (i == 0 || currKeyInt != prevKeyInt) {
                        out << "\"" << currKeyInt << ":[(" << currPageNum << "," << currSlotNum << ")";
                    }
                    else out << ",(" << currPageNum << "," << currSlotNum << ")";
                    if (i == numKeys-1) out << "]\"";

                    // go to next int
                    unsigned short sizePassed = INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                    prevKeyInt = currKeyInt;
                }
            }
            // attribute type is float
            else {
                float currKeyFlt;
                float prevKeyFlt = 0;
                for (int i = 0; i < numKeys; i++) {
                    // generate current float and rid
                    memcpy(&currKeyFlt, (char*) pageBuffer + currKeyPtr, FLT_SIZE);
                    memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr + FLT_SIZE, PTR_PN_SIZE);
                    memcpy(&currSlotNum, (char*) pageBuffer + currKeyPtr + FLT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);

                    // print current float and rid
                    if (i != 0 && currKeyFlt != prevKeyFlt) out << "]\",";
                    if (i == 0 || currKeyFlt != prevKeyFlt) {
                        out << "\"" << currKeyFlt << ":[(" << currPageNum << "," << currSlotNum << ")";
                    }
                    else out << ",(" << currPageNum << "," << currSlotNum << ")";
                    if (i == numKeys-1) out << "]\"";

                    // go to next float
                    unsigned short sizePassed = FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                    prevKeyFlt = currKeyFlt;
                }
            }
            out << "]}";
        }
        // node is non-leaf node
        else {
            out << "{\"keys\": [";
            unsigned short currKeyPtr = IS_LEAF_SIZE+PTR_PN_SIZE;
            std::vector<int> pageNumVector;
            int currPageNum;

            // put ptr to left most child page into pageNumVector
            memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr - PTR_PN_SIZE, PTR_PN_SIZE);
            pageNumVector.push_back(currPageNum);

            // attribute type is varChar
            if (attrType == TypeVarChar) {
                unsigned currVarCharLen;
                for (int i = 0; i < numKeys; i++) {
                    // generate current varChar
                    memcpy(&currVarCharLen, (char*) pageBuffer + currKeyPtr, VC_LEN_SIZE);
                    std::string currKeyVarChar = std::string((char *) pageBuffer + currKeyPtr + VC_LEN_SIZE, currVarCharLen);

                    // put ptr to child page associated with current varChar into pageNumVector
                    memcpy(&currPageNum,
                           (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE+PTR_SN_SIZE,
                           PTR_PN_SIZE);
                    pageNumVector.push_back(currPageNum);

                    // print current varChar
                    out << "\"" << currKeyVarChar << "\"";
                    if (i == numKeys - 1) {
                        out << "],\n";
                        for (int i = 0; i < indent+1; i++) out << " ";
                        out << "\"children\": [\n";
                    }
                    else out << ",";

                    // go to next varChar
                    unsigned short sizePassed = currVarCharLen + VC_LEN_SIZE + PTR_PN_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }
            // attribute type is int
            else if (attrType == TypeInt) {
                int currKeyInt;
                for (int i = 0; i < numKeys; i++) {
                    // generate current int
                    memcpy(&currKeyInt, (char *) pageBuffer + currKeyPtr, INT_SIZE);

                    // put ptr to child page associated with current int into pageNumVector
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE, PTR_PN_SIZE);
                    pageNumVector.push_back(currPageNum);

                    // print current int
                    out << "\"" << currKeyInt << "\"";
                    if (i == numKeys - 1) {
                        out << "],\n";
                        for (int i = 0; i < indent+1; i++) out << " ";
                        out << "\"children\": [\n";
                    }
                    else out << ",";

                    // go to next int
                    unsigned short sizePassed = INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }
            // attribute type is float
            else {
                float currKeyFlt;
                for (int i = 0; i < numKeys; i++) {
                    // generate current float
                    memcpy(&currKeyFlt, (char *) pageBuffer + currKeyPtr, FLT_SIZE);

                    // put ptr to child page associated with current float into pageNumVector
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE, PTR_PN_SIZE);
                    pageNumVector.push_back(currPageNum);

                    // print current float
                    out << "\"" << currKeyFlt << "\"";
                    if (i == numKeys - 1) {
                        out << "],\n";
                        for (int i = 0; i < indent+1; i++) out << " ";
                        out << "\"children\": [\n";
                    }
                    else out << ",";

                    // go to next float
                    unsigned short sizePassed = FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }

            // recursively print all child nodes
            int pageNumVectorSize = pageNumVector.size();
            for (int childPageNum : pageNumVector){
                printNode(ixFileHandle, attrType, childPageNum, indent+4, out);
                if (childPageNum != pageNumVector[pageNumVectorSize-1]) out << ",\n" ;
                else out << "\n";
            }
            for (int i = 0; i < indent; i++) out << " ";
            out << "]}";
        }
        free(pageBuffer);
        return 0;
    }

    /*************************************************/
    /*****    functions of ix_Scan_Iterator  *******/
    /*************************************************/
    RC IX_ScanIterator::initialize(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
                                   bool lowKeyInclusive, bool highKeyInclusive, IndexManager* ix){
        this->ix = ix;
        attrType = attribute.type;

        if (lowKey == NULL || lowKey == nullptr) this->lowKey = nullptr;
        else this->lowKey = lowKey;
        if (highKey == NULL || highKey == nullptr) this->highKey = nullptr;
        else this->highKey = highKey;

        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;
        this->ixFileHandle = &ixFileHandle;
        this->isFirstGetNextEntry = true;

        unsigned rootPageNum = this->ix->getRootPageNum(*this->ixFileHandle);
        this->currPageBuffer = malloc(PAGE_SIZE);
        return this->ixFileHandle->fileHandle.readPage(rootPageNum, currPageBuffer);
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if (ixFileHandle->fileHandle.getNumberOfPages() == 0) return -1;
        unsigned short numKeys = ix->getNumKeys(currPageBuffer);

        // if the first time call getNextEntry, need to find first leaf node from root node
        if (isFirstGetNextEntry) {
            int pageNumTobeScanned;
            //unsigned short currKeyPtr;
            isFirstGetNextEntry = false;

            bool isLeaf = ix->getIsLeaf(currPageBuffer);
            while (!isLeaf) {
                ixCurrKeyPtr = IS_LEAF_SIZE + PTR_PN_SIZE;

                // if lowKey is nullptr, go to the page pointed by left most ptr
                if (lowKey != nullptr) {
                    int keyIdx;
                    RC errCode = findEntryToOutput(keyIdx, numKeys, false);
                    if (errCode != 0) return errCode;
                }

                // read pageNumTobeScanned
                memcpy(&pageNumTobeScanned, (char*) currPageBuffer+ixCurrKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);
                RC errCode = ixFileHandle->fileHandle.readPage(pageNumTobeScanned, currPageBuffer);
                if (errCode != 0) return errCode;

                isLeaf = ix->getIsLeaf(currPageBuffer);
                numKeys = ix->getNumKeys(currPageBuffer);
            }

            // reached leaf node
            ixCurrKeyPtr = IS_LEAF_SIZE;
            int keyIdx = -1;

            // if lowKey is nullptr, output the leftmost entry (not violate highKey) directly
            if (lowKey != nullptr && numKeys != 0) {
                RC errCode = findEntryToOutput(keyIdx, numKeys, true);
                if (errCode != 0) return errCode;
            }

            // didn't find entry to output on current leaf node
            if (numKeys == 0 || keyIdx == numKeys) {
                RC errCode = findNextNonEmptyLeaf();
                if (errCode != 0) return errCode;
            }
        }
        // if not the first time call getNextEntry
        else {
            // freeBytes is a number stored, bytesLeft is the bytes after ixCurrKeyPtr
            unsigned short freeBytes = ix->getFreeBytes(currPageBuffer);
            unsigned short bytesLeft = PAGE_SIZE - ixCurrKeyPtr - NXT_PN_SIZE - N_SIZE - F_SIZE;
            //std::cout << "Inside getNextEntry, freeBytes is " << freeBytes << ", bytesLeft is " << bytesLeft << std::endl;

            // reached the last entry in leaf node
            if (freeBytes == bytesLeft) {
                RC errCode = findNextNonEmptyLeaf();
                if (errCode != 0) return errCode;
            }
        }

        // found entry to output (if not violate highKey) on current leaf node
        if (highKey != nullptr) {
            // attribute type is varChar
            if (attrType == TypeVarChar) {
                // generate high key varChar
                unsigned short highKeyLength = ix->getKeyLength(highKey, attrType);
                std::string highKeyVarChar = std::string((char*) highKey+VC_LEN_SIZE, highKeyLength-VC_LEN_SIZE);

                // generate current varChar
                unsigned currVarCharLen;
                memcpy(&currVarCharLen, (char*) currPageBuffer+ixCurrKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) currPageBuffer + ixCurrKeyPtr + VC_LEN_SIZE, currVarCharLen);

                if (highKeyInclusive) {
                    if (currKeyVarChar > highKeyVarChar) return IX_EOF;
                }
                else {
                    if (currKeyVarChar >= highKeyVarChar) return IX_EOF;
                }
            }
            // attribute type is int
            else if (attrType == TypeInt) {
                // generate high key int
                int highKeyInt = *(int*) this->highKey;

                // generate current int
                int currKeyInt;
                memcpy(&currKeyInt, (char*) currPageBuffer+ixCurrKeyPtr, INT_SIZE);

                if (highKeyInclusive) {
                    if (currKeyInt > highKeyInt) return IX_EOF;
                }
                else {
                    if (currKeyInt >= highKeyInt) return IX_EOF;
                }
            }
            // attribute type is float
            else {
                // generate high key float
                float highKeyFlt = *(float*) this->highKey;

                // generate current float
                float currKeyFlt;
                memcpy(&currKeyFlt, (char*) currPageBuffer+ixCurrKeyPtr, FLT_SIZE);

                if (highKeyInclusive) {
                    if (currKeyFlt > highKeyFlt) return IX_EOF;
                }
                else {
                    if (currKeyFlt >= highKeyFlt) return IX_EOF;
                }
            }
        }

        unsigned short keyLength = ix->getKeyLength((char*) currPageBuffer+ixCurrKeyPtr, attrType);
        memcpy((char*) key, (char*) currPageBuffer+ixCurrKeyPtr, keyLength);
        memcpy(&rid.pageNum, (char*) currPageBuffer+ixCurrKeyPtr+keyLength, PTR_PN_SIZE);
        memcpy(&rid.slotNum, (char*) currPageBuffer+ixCurrKeyPtr+keyLength+PTR_PN_SIZE, PTR_SN_SIZE);
        ixCurrKeyPtr += keyLength+PTR_PN_SIZE+PTR_SN_SIZE;
        return 0;
    }

    RC IX_ScanIterator::close() {
        free(currPageBuffer);
        return 0;
    }

    RC IX_ScanIterator::findNextNonEmptyLeaf() {
        int nextPageNum = ix->getNextPageNum(currPageBuffer);
        if (nextPageNum == -1) return IX_EOF;

        RC errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
        if (errCode != 0) return errCode;

        unsigned short numKeys = ix->getNumKeys(currPageBuffer);

        // find the next first leaf node that is not empty
        while (numKeys == 0){
            nextPageNum = ix->getNextPageNum(currPageBuffer);
            if (nextPageNum == -1)  return IX_EOF;

            RC errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
            if (errCode != 0) return errCode;

            numKeys = ix->getNumKeys(currPageBuffer);
        }

        ixCurrKeyPtr = IS_LEAF_SIZE;
        return 0;
    }

    RC IX_ScanIterator::findEntryToOutput(int& keyIdx, unsigned short numKeys, bool isLeaf) {
        // attribute type is varChar
        if (attrType == TypeVarChar) {
            // generate low key varChar
            unsigned short lowKeyLength = ix->getKeyLength(lowKey, attrType);
            std::string lowKeyVarChar = std::string((char*) lowKey+VC_LEN_SIZE, lowKeyLength-VC_LEN_SIZE);

            unsigned currVarCharLen;
            for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                // generate current varChar
                memcpy(&currVarCharLen, (char*) currPageBuffer+ixCurrKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) currPageBuffer+ixCurrKeyPtr+VC_LEN_SIZE, currVarCharLen);

                // break if found entry to output
                if (lowKeyInclusive){
                    if (lowKeyVarChar <= currKeyVarChar) break;
                }
                else {
                    if (lowKeyVarChar < currKeyVarChar) break;
                }

                // not found, go to next varChar
                unsigned short sizePassed;
                if (isLeaf) sizePassed = VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizePassed = VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                ixCurrKeyPtr += sizePassed;
            }
        }
            // attribute type is int
        else if (attrType == TypeInt) {
            // generate low key int
            int lowKeyInt = *(int*) this->lowKey;

            int currKeyInt;
            for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                // generate current int
                memcpy(&currKeyInt, (char*) currPageBuffer+ixCurrKeyPtr, INT_SIZE);

                // break if found entry to output
                if (lowKeyInclusive){
                    if (lowKeyInt <= currKeyInt) break;
                }
                else {
                    if (lowKeyInt < currKeyInt) break;
                }

                // not found, go to next int
                unsigned short sizePassed;
                if (isLeaf) sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                ixCurrKeyPtr += sizePassed;
            }
        }
            // attribute type is float
        else {
            // generate low key float
            float lowKeyFlt = *(float*) this->lowKey;

            float currKeyFlt;
            for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                // generate current float
                memcpy(&currKeyFlt, (char*) currPageBuffer+ixCurrKeyPtr, FLT_SIZE);

                // break if found entry to output
                if (lowKeyInclusive){
                    if (lowKeyFlt <= currKeyFlt) break;
                }
                else {
                    if (lowKeyFlt < currKeyFlt) break;
                }

                // not found, go to next float
                unsigned short sizePassed;
                if (isLeaf) sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                ixCurrKeyPtr += sizePassed;
            }
        }
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() = default;

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        RC errCode = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
        if (errCode != 0) return errCode;

        ixReadPageCounter = readPageCount;
        ixWritePageCounter = writePageCount;
        ixAppendPageCounter = appendPageCount;
        return 0;
    }

} // namespace PeterDB