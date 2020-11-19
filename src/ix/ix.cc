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
        unsigned keyLength;
        if (attrType == TypeVarChar) {
            memcpy(&keyLength, key, VC_LEN_SIZE);
            keyLength += VC_LEN_SIZE;
        }
        else {
            keyLength = INT_OR_FLT_SIZE;
        }

        // B+ tree has 0 node
        if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
            // set up meta data page
            void* metaDataPageBuffer = malloc(PAGE_SIZE);
            unsigned rootPageNum = 1;
            memcpy((char*) metaDataPageBuffer, &rootPageNum, ROOT_PAGE_NUM_SIZE);
            RC errCode = ixFileHandle.fileHandle.appendPage(metaDataPageBuffer);
            if (errCode != 0) return errCode;
            free(metaDataPageBuffer);

            unsigned short bytesNeeded = keyLength + PTR_PN_SIZE + PTR_SN_SIZE;
            void* firstEntry = malloc(bytesNeeded);
            memcpy((char*) firstEntry, (char*) key, keyLength);
            memcpy((char*) firstEntry+keyLength, &rid.pageNum, PTR_PN_SIZE);
            memcpy((char*) firstEntry+keyLength+PTR_PN_SIZE, &rid.slotNum, PTR_SN_SIZE);

            errCode = initLeafNode(pageBuffer, firstEntry, bytesNeeded, 1, -1);
            if (errCode != 0) return errCode;

            free(firstEntry);
            errCode = ixFileHandle.fileHandle.appendPage(pageBuffer);
            if (errCode != 0) return errCode;

            return 0;
        }
        // B+ tree has at least 1 node
        else {
            unsigned rootPageNum = getRootPageNum(ixFileHandle);
            //std::cout << "Inside insertEntry, rootPageNum is " << rootPageNum << std::endl;

            RC errCode = ixFileHandle.fileHandle.readPage(rootPageNum, pageBuffer);
            if (errCode != 0) return errCode;
            //void* newChildEntry = nullptr;

            void* newChildEntry = malloc(PAGE_SIZE);
            errCode = insertEntry1(ixFileHandle, pageBuffer, rootPageNum, keyLength,
                                   attrType, key, rid, newChildEntry, rootPageNum);
            //std::cout << "Inside insertEntry, after insertEntry1, errCode is " << errCode << std::endl;
            if (errCode != 0) return errCode;

            free(newChildEntry);
        }
        free(pageBuffer);
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return ix_ScanIterator.initialize(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        unsigned rootPageNum = getRootPageNum(ixFileHandle);
        return printNode(ixFileHandle, attribute.type, rootPageNum, 0, out);
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    unsigned IndexManager::getRootPageNum(IXFileHandle &ixFileHandle) const {
        void* pageBuffer = malloc(PAGE_SIZE);
        unsigned rootPageNum;
        RC errCode = ixFileHandle.fileHandle.readPage(0, pageBuffer);
        if (errCode != 0) return errCode;

        memcpy(&rootPageNum, (char*) pageBuffer, ROOT_PAGE_NUM_SIZE);
        free(pageBuffer);
        return rootPageNum;
    }

    RC IndexManager::insertEntry1(IXFileHandle &ixFileHandle, void* pageBuffer, unsigned pageNum,
                                  unsigned keyLength, AttrType attrType, const void *key,
                                  const RID &rid, void* &newChildEntry, unsigned rootPageNum) {
        bool isRoot = pageNum==rootPageNum;
        bool isLeaf = getIsLeaf(pageBuffer);
        unsigned short freeBytes = getFreeBytes(pageBuffer);
        unsigned short numKeys = getNumKeys(pageBuffer);
        std::cout << "Inside insertEntry1, isLeaf? " << isLeaf << std::endl;

        if (isLeaf) {
            unsigned short bytesNeeded = keyLength + PTR_PN_SIZE + PTR_SN_SIZE;
            // new key can be inserted in this leaf node
            if (bytesNeeded <= freeBytes) {
                RC errCode = insertEntry2(pageBuffer, keyLength, key, rid, bytesNeeded,
                                          freeBytes, numKeys, attrType, true);
                if(errCode != 0) return errCode;
                //free(newChildEntry);
                newChildEntry = nullptr;

                setFreeBytes(pageBuffer, freeBytes-bytesNeeded);
                setNumKeys(pageBuffer, numKeys+1);
                return ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
            }
            // new key cannot be inserted in this leaf node, must split
            else {
                return splitNode(ixFileHandle, pageBuffer, keyLength, attrType, key, rid, newChildEntry,
                                     pageNum, bytesNeeded, freeBytes, numKeys, true, isRoot);
            }
        }
        // node is non-leaf node
        else {
            int pageNumToBeInserted;
            std::cout<< "inside insertEntry1, before findPageNumToBeHandled " << std::endl;
            RC errCode = findPageNumToBeHandled(pageBuffer, keyLength, key, rid,
                                                numKeys, attrType, pageNumToBeInserted);
            std::cout<< "inside insertEntry1, after findPageNumToBeHandled pageNumToBeInserted is " << pageNumToBeInserted << std::endl;
            if(errCode != 0) return errCode;

            errCode = ixFileHandle.fileHandle.readPage(pageNumToBeInserted, pageBuffer);
            if(errCode != 0) return errCode;

            errCode = insertEntry1(ixFileHandle, pageBuffer, pageNumToBeInserted,
                                   keyLength, attrType, key, rid, newChildEntry, rootPageNum);
            if(errCode != 0) return errCode;
            //std::cout<< "inside insertEntry1, after findPageNumToBeHandled newChildEntry == nullptr is " << (newChildEntry == nullptr) << std::endl;

            if (newChildEntry == nullptr) return 0;
            else {
                errCode = ixFileHandle.fileHandle.readPage(pageNum, pageBuffer);
                if(errCode != 0) return errCode;

                unsigned newChildKeyLength;
                if (attrType == TypeVarChar) {
                    unsigned varCharLen;
                    memcpy(&varCharLen, (char*) newChildEntry, VC_LEN_SIZE);
                    newChildKeyLength = VC_LEN_SIZE+varCharLen;
                    std::cout << "Inside insertEntry1, varCharLen to be inserted is " << varCharLen << std::endl;
                }
                else {
                    newChildKeyLength = INT_OR_FLT_SIZE;
                }
                unsigned bytesNeeded = newChildKeyLength+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;

                RID newChildEntryRid;
                memcpy(&newChildEntryRid.pageNum, (char*) newChildEntry+newChildKeyLength, PTR_PN_SIZE);
                memcpy(&newChildEntryRid.slotNum, (char*) newChildEntry+newChildKeyLength+PTR_PN_SIZE, PTR_SN_SIZE);

                if (bytesNeeded <= freeBytes) {
                    RC errCode = insertEntry2(pageBuffer, bytesNeeded, newChildEntry, newChildEntryRid, bytesNeeded,
                                              freeBytes, numKeys, attrType,false);
                    if(errCode != 0) return errCode;
                    //free(newChildEntry);
                    newChildEntry = nullptr;

                    setFreeBytes(pageBuffer, freeBytes-bytesNeeded);
                    setNumKeys(pageBuffer, numKeys+1);
                    std::cout << "Inside insertEntry1, child split, newChildEntry inserted to parent，numKeys is " << numKeys+1 << std::endl;
                    return ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
                }
                else {
                    return splitNode(ixFileHandle, pageBuffer, bytesNeeded, attrType, newChildEntry, newChildEntryRid, newChildEntry,
                                     pageNum, bytesNeeded, freeBytes, numKeys, false, isRoot);
                }
            }
        }
    }

    void IndexManager::shiftKey(void* pageBuffer, short keyOffset, short sizeToBeShifted, unsigned short distance) {
        memcpy((char*) pageBuffer+keyOffset+distance, (char*) pageBuffer+keyOffset, sizeToBeShifted);
    }

    RC IndexManager::insertEntry2(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                                  unsigned short bytesNeeded, unsigned short freeBytes, unsigned short numKeys,
                                  AttrType attrType, bool isLeaf){
        bool insertHere = false;
        unsigned short currKeyPtr;
        unsigned short sizeToBeShifted;
        if (isLeaf) {
            currKeyPtr = IS_LEAF_SIZE;
            sizeToBeShifted = PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE-freeBytes-IS_LEAF_SIZE;
        }
        else {
            currKeyPtr = IS_LEAF_SIZE + PTR_PN_SIZE;
            sizeToBeShifted = PAGE_SIZE-F_SIZE-N_SIZE-freeBytes-PTR_PN_SIZE-IS_LEAF_SIZE;
        }

        unsigned currPageNum;
        unsigned short currSlotNum;
        if (attrType == TypeVarChar) {
            std::string newKeyVarChar = std::string((char*) key+VC_LEN_SIZE, keyLength-VC_LEN_SIZE);
            unsigned currVarCharLen;
            for (int i = 0; i < numKeys; i++) {
                memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);
                if (newKeyVarChar == currKeyVarChar) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) insertHere = true;
                    }
                    else if (rid.pageNum < currPageNum) insertHere = true;
                }
                else if (newKeyVarChar < currKeyVarChar) insertHere = true;

                if (insertHere) {
                    return insertEntry3(pageBuffer, keyLength, key, rid, currKeyPtr,
                                        sizeToBeShifted, bytesNeeded, isLeaf);
                }
                else {
                    unsigned short sizePassed;
                    if (isLeaf) sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                    else sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                    sizeToBeShifted -= sizePassed;
                    currKeyPtr += sizePassed;
                }
            }
            // after for loop
            return insertEntry3(pageBuffer, keyLength, key, rid, currKeyPtr,
                                sizeToBeShifted, bytesNeeded, isLeaf);
        }
        else if (attrType == TypeInt) {
            int newKeyInt;
            memcpy(&newKeyInt, (char*) key, INT_SIZE);
            for (int i = 0; i < numKeys; i++) {
                int currKeyInt;
                memcpy(&currKeyInt, (char*) pageBuffer+currKeyPtr, INT_SIZE);
                if (newKeyInt == currKeyInt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) insertHere = true;
                    }
                    else if (rid.pageNum < currPageNum) insertHere = true;
                }
                else if (newKeyInt < currKeyInt) insertHere = true;

                if (insertHere) {
                    return insertEntry3(pageBuffer, keyLength, key, rid, currKeyPtr,
                                        sizeToBeShifted, bytesNeeded, isLeaf);
                }
                else {
                    unsigned short sizePassed;
                    if (isLeaf) sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                    else sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                    sizeToBeShifted -= sizePassed;
                    currKeyPtr += sizePassed;
                }
            }
            // after for loop
            return insertEntry3(pageBuffer, keyLength, key, rid, currKeyPtr,
                                sizeToBeShifted, bytesNeeded, isLeaf);
        }
        else {
            float newKeyFlt;
            memcpy(&newKeyFlt, (char*) key, FLT_SIZE);
            for (int i = 0; i < numKeys; i++) {
                float currKeyFlt;
                memcpy(&currKeyFlt, (char*) pageBuffer+currKeyPtr, FLT_SIZE);
                if (newKeyFlt == currKeyFlt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currSlotNum) insertHere = true;
                    }
                    else if (rid.pageNum < currPageNum) insertHere = true;
                }
                else if (newKeyFlt < currKeyFlt) insertHere = true;

                if (insertHere) {
                    return insertEntry3(pageBuffer, keyLength, key, rid, currKeyPtr,
                                        sizeToBeShifted, bytesNeeded, isLeaf);
                }
                else {
                    unsigned short sizePassed;
                    if (isLeaf) sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                    else sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                    sizeToBeShifted -= sizePassed;
                    currKeyPtr += sizePassed;
                }
            }
            // after for loop
            return insertEntry3(pageBuffer, keyLength, key, rid, currKeyPtr,
                                sizeToBeShifted, bytesNeeded, isLeaf);
        }
    }

    RC IndexManager::insertEntry3(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                                  short currKeyPtr, short sizeToBeShifted, unsigned short bytesNeeded, bool isLeaf) {
        shiftKey(pageBuffer, currKeyPtr, sizeToBeShifted, bytesNeeded);
        memcpy((char*) pageBuffer+currKeyPtr, (char*) key, keyLength);
        if (isLeaf) {
            memcpy((char*) pageBuffer+currKeyPtr+keyLength, &rid.pageNum, PTR_PN_SIZE);
            memcpy((char*) pageBuffer+currKeyPtr+keyLength+PTR_PN_SIZE, &rid.slotNum, PTR_SN_SIZE);
        }
        return 0;
    }

    RC IndexManager::findPageNumToBeHandled(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                                             unsigned short numKeys, AttrType attrType, int& pageNumToBeInserted) {
        unsigned short currKeyPtr = IS_LEAF_SIZE + PTR_PN_SIZE;
        unsigned currPageNum;
        unsigned short currSlotNum;
        if (attrType == TypeVarChar) {
            std::string newKeyVarChar = std::string((char*) key+VC_LEN_SIZE, keyLength-VC_LEN_SIZE);
            std::cout << "Inside findPageNumToBeHandled, newKeyVarChar is " << newKeyVarChar << std::endl;
            unsigned currVarCharLen;
            for (int i = 0; i < numKeys; i++) {
                memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                std::cout << "Inside findPageNumToBeHandled finding insert point, currVarCharLen is "<< currVarCharLen << std::endl;
                std::string currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);
                if (newKeyVarChar == currKeyVarChar) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currPageNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyVarChar < currKeyVarChar) break;

                unsigned short sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                currKeyPtr += sizePassed;
            }
            // after for loop
            memcpy(&pageNumToBeInserted, (char*) pageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);/////(char*) pageBuffer+currKeyPtr-PTR_PN_SIZE
        }
        else if (attrType == TypeInt) {
            int newKeyInt;
            memcpy(&newKeyInt, (char*) key, INT_SIZE);
            for (int i = 0; i < numKeys; i++) {
                int currKeyInt;
                memcpy(&currKeyInt, (char*) pageBuffer+currKeyPtr, INT_SIZE);
                //std::cout<<"inside findpageNumToBehandled loop, currkeyInt is "<<currKeyInt<<" numKeys is " <<numKeys << " i is " << i<<std::endl;
                if (newKeyInt == currKeyInt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currPageNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyInt < currKeyInt) break;

                unsigned short sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                currKeyPtr += sizePassed;
            }
            // after for loop
            memcpy(&pageNumToBeInserted, (char*) pageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);/////(char*) pageBuffer+currKeyPtr-PTR_PN_SIZE
        }
        else {
            float newKeyFlt;
            memcpy(&newKeyFlt, (char*) key, FLT_SIZE);
            for (int i = 0; i < numKeys; i++) {
                float currKeyFlt;
                memcpy(&currKeyFlt, (char*) pageBuffer+currKeyPtr, FLT_SIZE);
                if (newKeyFlt == currKeyFlt) {
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE, PTR_PN_SIZE);
                    if (rid.pageNum == currPageNum) {
                        memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE, PTR_SN_SIZE);
                        // Equal slotNum is impossible
                        if (rid.slotNum < currPageNum) break;
                    }
                    else if (rid.pageNum < currPageNum) break;
                }
                else if (newKeyFlt < currKeyFlt) break;

                unsigned short sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                currKeyPtr += sizePassed;
            }
            // after for loop
            memcpy(&pageNumToBeInserted, (char*) pageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);/////(char*) pageBuffer+currKeyPtr-PTR_PN_SIZE
        }
        return 0;
    }

    RC IndexManager::splitNode(IXFileHandle &ixFileHandle, void* pageBuffer, unsigned keyLength,
                               AttrType attrType, const void *key, const RID &rid, void* &newChildEntry,
                               unsigned pageNum, unsigned short bytesNeeded,unsigned short freeBytes,
                               unsigned short numKeys, bool isLeaf, bool isRoot) {
        void* hugePageBuffer = malloc(2*PAGE_SIZE);
        memcpy(hugePageBuffer, pageBuffer, PAGE_SIZE);
        RC errCode = insertEntry2(hugePageBuffer, keyLength, key, rid, bytesNeeded, freeBytes, numKeys, attrType, isLeaf);
        if(errCode != 0) return errCode;
        std::cout << "Inside splitNode, after insert entry into huge buffer" << std::endl;

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
        if (attrType == TypeVarChar) {
            unsigned currVarCharLen;
            for (keyIdx = 0; keyIdx < numKeys+1; keyIdx++) {
                memcpy(&currVarCharLen, (char*) hugePageBuffer+currKeyPtr, VC_LEN_SIZE);
                std::cout << "Inside splitNode finding split point, currVarCharLen is " << currVarCharLen << std::endl;
                if (isLeaf) sizeToBePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                else sizeToBePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;

                if (sizeToBeCopied < ORDER) {
                    unsigned short leftSiblingFreeBytes;
                    memcpy((char*)pageBuffer, (char*)hugePageBuffer, PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE);
                    setNumKeys(pageBuffer, keyIdx);
                    if (isLeaf) leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr-F_SIZE-N_SIZE-NXT_PN_SIZE;
                    else leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr-F_SIZE-N_SIZE;
                    setFreeBytes(pageBuffer, leftSiblingFreeBytes);

                    //if (isLeaf) newChildEntry = malloc(sizeToBePassed+PTR_PN_SIZE);
                    //else newChildEntry = malloc(sizeToBePassed);

                    memcpy((char*) newChildEntry, (char*) hugePageBuffer+currKeyPtr, sizeToBePassed);
                    break;
                }
                else {
                    sizeToBeCopied -= sizeToBePassed;
                    currKeyPtr += sizeToBePassed;
                }
            }
        }
        else {
            if (isLeaf) sizeToBePassed = INT_OR_FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
            else sizeToBePassed = INT_OR_FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;

            for (keyIdx = 0; keyIdx < numKeys+1; keyIdx++) {
                if (sizeToBeCopied < ORDER) {
                    unsigned short leftSiblingFreeBytes;
                    memcpy((char*)pageBuffer, (char*)hugePageBuffer, PAGE_SIZE-F_SIZE-N_SIZE-NXT_PN_SIZE);
                    setNumKeys(pageBuffer, keyIdx);
                    if (isLeaf) leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr-F_SIZE-N_SIZE-NXT_PN_SIZE;
                    else leftSiblingFreeBytes = PAGE_SIZE-currKeyPtr-F_SIZE-N_SIZE;
                    setFreeBytes(pageBuffer, leftSiblingFreeBytes);

                    //if (newChildEntry != nullptr) free(newChildEntry);
                    //if (isLeaf) newChildEntry = malloc(sizeToBePassed+PTR_PN_SIZE);
                    //else newChildEntry = malloc(sizeToBePassed);

                    memcpy((char*) newChildEntry, (char*) hugePageBuffer+currKeyPtr, sizeToBePassed);
                    break;
                }
                else {
                    sizeToBeCopied -= sizeToBePassed;
                    currKeyPtr += sizeToBePassed;
                }
            }
        }

        // prepare right sibling
        int leftSiblingNxtPageNum = getNextPageNum(pageBuffer);
        void* rightSiblingPageBuffer = malloc(PAGE_SIZE);
        if (isLeaf) {
            errCode = initLeafNode(rightSiblingPageBuffer, (char*) hugePageBuffer+currKeyPtr,
                                   sizeToBeCopied, numKeys+1-keyIdx, leftSiblingNxtPageNum);
        }
        else {
            errCode = initNonLeafNode(rightSiblingPageBuffer, (char*) hugePageBuffer+currKeyPtr+sizeToBePassed-PTR_PN_SIZE,   //////
                                      sizeToBeCopied-sizeToBePassed+PTR_PN_SIZE, numKeys-keyIdx);   ////////
        }
        if(errCode != 0) return errCode;
        free(hugePageBuffer);

        errCode = ixFileHandle.fileHandle.appendPage(rightSiblingPageBuffer);
        if(errCode != 0) return errCode;
        free(rightSiblingPageBuffer);
        std::cout << "Inside splitNode, finish prepare right sibling" << std::endl;

        // finish prepare left sibling
        int rightSiblingPageNum = ixFileHandle.fileHandle.getNumberOfPages()-1;
        if (isLeaf) {
            memcpy((char*) newChildEntry+sizeToBePassed, &rightSiblingPageNum, PTR_PN_SIZE);
            setNextPageNum(pageBuffer, rightSiblingPageNum);
        }
        else{
            memcpy((char*) newChildEntry+sizeToBePassed-PTR_PN_SIZE, &rightSiblingPageNum, PTR_PN_SIZE);
        }
        ////////////////debug/////////
        unsigned newChildVarCharLen;
        memcpy(&newChildVarCharLen, (char*) newChildEntry, VC_LEN_SIZE);
        std::cout << "Inside splitNode, newChildEntry key is " << newChildVarCharLen << std::endl;
        ///////////////debug//////////
        errCode = ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
        if(errCode != 0) return errCode;

        if (isRoot) {
            void* newRootPageBuffer = malloc(PAGE_SIZE);

            unsigned short newRootEntryLength;
            if (isLeaf) newRootEntryLength = sizeToBePassed+PTR_PN_SIZE;
            else newRootEntryLength = sizeToBePassed;

            void* newRootEntry = malloc(PTR_PN_SIZE+newRootEntryLength);
            memcpy((char*) newRootEntry, &pageNum, PTR_PN_SIZE);
            memcpy((char*) newRootEntry+PTR_PN_SIZE, (char*) newChildEntry, newRootEntryLength);

            std::cout << "Inside splitNode, before init new root page" << std::endl;
            errCode = initNonLeafNode(newRootPageBuffer, newRootEntry, PTR_PN_SIZE+newRootEntryLength, 1);
            if(errCode != 0) return errCode;

            std::cout << "Inside splitNode, after init new root page" << std::endl;
            errCode = ixFileHandle.fileHandle.appendPage(newRootPageBuffer);
            if(errCode != 0) return errCode;

            unsigned newRootPageNum = ixFileHandle.fileHandle.getNumberOfPages()-1;
            errCode = ixFileHandle.fileHandle.readPage(0, pageBuffer);
            if (errCode != 0) return errCode;

            memcpy((char*) pageBuffer, &newRootPageNum, sizeof(unsigned));
            errCode = ixFileHandle.fileHandle.writePage(0, pageBuffer);
            if (errCode != 0) return errCode;
            std::cout << "Inside splitNode, after update root pageNum" << std::endl;

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
        void *pageBuffer = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum, pageBuffer);

        bool isLeaf = getIsLeaf(pageBuffer);
        unsigned short numKeys = getNumKeys(pageBuffer);

        for (int i = 0; i < indent; i++) out << " ";

        if (isLeaf) {
            unsigned short currKeyPtr = IS_LEAF_SIZE;
            unsigned currPageNum;
            unsigned short currSlotNum;
            out << "{\"keys\": [";
            if (attrType == TypeVarChar) {
                unsigned currVarCharLen;
                for (int i = 0; i < numKeys; i++) {
                    memcpy(&currVarCharLen, (char*) pageBuffer + currKeyPtr, VC_LEN_SIZE);
                    std::string currKeyVarChar = std::string((char *) pageBuffer + currKeyPtr + VC_LEN_SIZE,
                                                             currVarCharLen);
                    memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr + VC_LEN_SIZE + currVarCharLen, PTR_PN_SIZE);
                    memcpy(&currSlotNum, (char*) pageBuffer + currKeyPtr + VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE,
                           PTR_SN_SIZE);
                    out << "\"" << currKeyVarChar << ":[(" << currPageNum << "," << currSlotNum << ")]\"";
                    if (i == numKeys - 1) out << "]}";
                    else out << ",";

                    unsigned short sizePassed = currVarCharLen + VC_LEN_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                }
            } else if (attrType == TypeInt) {
                int currKeyInt;
                for (int i = 0; i < numKeys; i++) {
                    memcpy(&currKeyInt, (char*) pageBuffer + currKeyPtr, INT_SIZE);
                    memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr + INT_SIZE, PTR_PN_SIZE);
                    memcpy(&currSlotNum, (char*) pageBuffer + currKeyPtr + INT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);

                    out << "\"" << currKeyInt << ":[(" << currPageNum << "," << currSlotNum << ")]\"";
                    if (i == numKeys - 1) out << "]}";
                    else out << ",";

                    unsigned short sizePassed = INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                }
            } else {
                float currKeyFlt;
                for (int i = 0; i < numKeys; i++) {
                    memcpy(&currKeyFlt, (char*) pageBuffer + currKeyPtr, FLT_SIZE);
                    memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr + FLT_SIZE, PTR_PN_SIZE);
                    memcpy(&currSlotNum, (char*) pageBuffer + currKeyPtr + FLT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);

                    out << "\"" << currKeyFlt << ":[(" << currPageNum << "," << currSlotNum << ")]\"";
                    if (i == numKeys - 1) out << "]}";
                    else out << ",";

                    unsigned short sizePassed = FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }
        }
        else {
            out << "{\"keys\": [";
            unsigned short currKeyPtr = IS_LEAF_SIZE+PTR_PN_SIZE;
            std::vector<int> pageNumVector;

            if (attrType == TypeVarChar) {
                unsigned currVarCharLen;
                int currPageNum;
                memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr - PTR_PN_SIZE, PTR_PN_SIZE);
                pageNumVector.push_back(currPageNum);
                for (int i = 0; i < numKeys; i++) {
                    memcpy(&currVarCharLen, (char*) pageBuffer + currKeyPtr, VC_LEN_SIZE);
                    std::string currKeyVarChar = std::string((char *) pageBuffer + currKeyPtr + VC_LEN_SIZE, currVarCharLen);
                    memcpy(&currPageNum,
                           (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE+PTR_SN_SIZE,
                           PTR_PN_SIZE);
                    pageNumVector.push_back(currPageNum);

                    out << "\"" << currKeyVarChar << "\"";
                    if (i == numKeys - 1) {
                        out << "],\n";
                        for (int i = 0; i < indent+1; i++) out << " ";
                        out << "\"children\": [\n";
                    }
                    else out << ",";

                    unsigned short sizePassed = currVarCharLen + VC_LEN_SIZE + PTR_PN_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }
            else if (attrType == TypeInt) {
                int currKeyInt;
                int currPageNum;
                memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr - PTR_PN_SIZE, PTR_PN_SIZE);
                pageNumVector.push_back(currPageNum);
                for (int i = 0; i < numKeys; i++) {
                    memcpy(&currKeyInt, (char *) pageBuffer + currKeyPtr, INT_SIZE);
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE, PTR_PN_SIZE);
                    pageNumVector.push_back(currPageNum);

                    out << "\"" << currKeyInt << "\"";
                    if (i == numKeys - 1) {
                        out << "],\n";
                        for (int i = 0; i < indent+1; i++) out << " ";
                        out << "\"children\": [\n";
                    }
                    else out << ",";

                    unsigned short sizePassed = INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }
            else {
                float currKeyFlt;
                int currPageNum;
                memcpy(&currPageNum, (char*) pageBuffer + currKeyPtr - PTR_PN_SIZE, PTR_PN_SIZE);
                pageNumVector.push_back(currPageNum);
                for (int i = 0; i < numKeys; i++) {
                    memcpy(&currKeyFlt, (char *) pageBuffer + currKeyPtr, FLT_SIZE);
                    memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE, PTR_PN_SIZE);
                    pageNumVector.push_back(currPageNum);

                    out << "\"" << currKeyFlt << "\"";
                    if (i == numKeys - 1) {
                        out << "],\n";
                        for (int i = 0; i < indent+1; i++) out << " ";
                        out << "\"children\": [\n";
                    }
                    else out << ",";

                    unsigned short sizePassed = FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                    currKeyPtr += sizePassed;
                }
            }
            int pageNumVectorSize = pageNumVector.size();
            for (int pageNum : pageNumVector){
                printNode(ixFileHandle, attrType, pageNum, indent+4, out);
                if (pageNum != pageNumVector[pageNumVectorSize-1]) out << ",\n" ;
                else out << "\n";
            }

            for (int i = 0; i < indent; i++) out << " ";
            out << "]}";
        }
        free(pageBuffer);
        return 0;
    }

    /*********************************************/
    /*****    Getter and Setter functions  *******/
    /*********************************************/
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

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    /*************************************************/
    /*****    functions of ix_Scan_Iterator  *******/
    /*************************************************/

    RC IX_ScanIterator::initialize(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                   const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive){
        IndexManager &ix = IndexManager::instance();
        attrType = attribute.type;

        if (lowKey == NULL){
            if (attrType == TypeVarChar) {
                unsigned attrLenth = attribute.length;
                //TO DO
            }
            else if (attrType == TypeInt) this->lowKey = &minInt;
            else this->lowKey = &minFlt;
        }
        else this->lowKey = lowKey;

        if (highKey == NULL){
            if (attrType == TypeVarChar) {
                unsigned attrLenth = attribute.length;
                //TO DO
            }
            else if (attrType == TypeInt) this->highKey = &maxInt;
            else this->highKey = &maxFlt;
        }
        else this->highKey = highKey;

        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;
        this->ixFileHandle = &ixFileHandle;
        this->ixCurrKeyPtr = 0;
        this->isFirstGetNextEntry = true;
        //this->ixCurrPageNum = ix.getRootPageNum(*this->ixFileHandle);

        int rootPageNum = ix.getRootPageNum(*this->ixFileHandle);
        this->currPageBuffer = malloc(PAGE_SIZE);
        return this->ixFileHandle->fileHandle.readPage(rootPageNum, currPageBuffer);
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if (ixFileHandle->fileHandle.getNumberOfPages() == 0) return -1;
        IndexManager &ix = IndexManager::instance();
        //unsigned rootPageNum = ix.getRootPageNum(*ixFileHandle);
        //void* currPageBuffer = malloc(PAGE_SIZE);
        //RC errCode = ixFileHandle->fileHandle.readPage(ixCurrPageNum, currPageBuffer);
        //if (errCode != 0) return errCode;

        unsigned short numKeys = ix.getNumKeys(currPageBuffer);
        unsigned lowKeyLength;
        unsigned highKeyLength;

        if (attrType == TypeVarChar) {
            memcpy(&lowKeyLength, lowKey, VC_LEN_SIZE);
            lowKeyLength += VC_LEN_SIZE;
            memcpy(&highKeyLength, highKey, VC_LEN_SIZE);
            highKeyLength += VC_LEN_SIZE;
        }
        else {
            lowKeyLength = INT_OR_FLT_SIZE;
            highKeyLength = INT_OR_FLT_SIZE;
        }

        unsigned pageNum;
        unsigned short slotNum;
        if (!isFirstGetNextEntry) {
            unsigned short freeBytes = ix.getFreeBytes(currPageBuffer);
            unsigned short bytesLeft = PAGE_SIZE - ixCurrKeyPtr - NXT_PN_SIZE - N_SIZE - F_SIZE;
            std::cout << "Inside getNextEntry, freeBytes is " << freeBytes << ", bytesLeft is " << bytesLeft << std::endl;
            if (freeBytes == bytesLeft){
                int nextPageNum = ix.getNextPageNum(currPageBuffer);
                std::cout << "Inside getNextEntry, nextPageNum is " << nextPageNum << std::endl;
                if (nextPageNum == -1) return IX_EOF;
                RC errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
                if (errCode != 0) return errCode;
                numKeys = ix.getNumKeys(currPageBuffer);
                while(numKeys == 0){
                    nextPageNum = ix.getNextPageNum(currPageBuffer);
                    if (nextPageNum == -1)  return IX_EOF;
                    RC errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
                    if (errCode != 0) return errCode;
                    numKeys = ix.getNumKeys(currPageBuffer);
                }
                ixCurrKeyPtr = IS_LEAF_SIZE;
                //ixCurrPageNum = nextPageNum;
            }
            if (attrType == TypeVarChar) {
                unsigned currVarCharLen;
                memcpy(&currVarCharLen, (char*) currPageBuffer+ixCurrKeyPtr, VC_LEN_SIZE);
                std::string currKeyVarChar = std::string((char*) currPageBuffer+ixCurrKeyPtr+VC_LEN_SIZE, currVarCharLen);
                std::string highKeyVarChar = std::string((char*) highKey+VC_LEN_SIZE, highKeyLength-VC_LEN_SIZE);
                if (highKeyInclusive){
                    if (currKeyVarChar <= highKeyVarChar){
                        memcpy((char* )key,  (char* )currPageBuffer + ixCurrKeyPtr, VC_LEN_SIZE + currVarCharLen);
                        memcpy(&pageNum, (char* )currPageBuffer + ixCurrKeyPtr + VC_LEN_SIZE + currVarCharLen, PTR_PN_SIZE);
                        memcpy(&slotNum, (char* )currPageBuffer + ixCurrKeyPtr + VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE, PTR_SN_SIZE);
                        ixCurrKeyPtr += VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE + PTR_SN_SIZE;
                    }
                    else{
                        return IX_EOF;
                    }
                }
                else{
                    if (currKeyVarChar < highKeyVarChar){
                        memcpy((char* )key,  (char* )currPageBuffer + ixCurrKeyPtr, VC_LEN_SIZE + currVarCharLen);
                        memcpy(&pageNum, (char* )currPageBuffer + ixCurrKeyPtr + VC_LEN_SIZE + currVarCharLen, PTR_PN_SIZE);
                        memcpy(&slotNum, (char* )currPageBuffer + ixCurrKeyPtr + VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE, PTR_SN_SIZE);
                        ixCurrKeyPtr += VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE + PTR_SN_SIZE;
                    }
                    else{
                        return IX_EOF;
                    }
                }
            }
            else if (attrType == TypeInt) {
                int currKeyInt;
                memcpy(&currKeyInt, (char*) currPageBuffer+ixCurrKeyPtr, INT_SIZE);
                std::cout << "Inside getNextEntry, in preparing int, currKeyInt is " << currKeyInt << std::endl;
                int highKeyInt;
                memcpy(&highKeyInt, (char*) highKey, INT_SIZE);
                if (highKeyInclusive){
                    if (currKeyInt <= highKeyInt){
                        memcpy((char* )key,  (char* )currPageBuffer + ixCurrKeyPtr, INT_SIZE);
                        memcpy(&pageNum, (char* )currPageBuffer + ixCurrKeyPtr + INT_SIZE, PTR_PN_SIZE);
                        memcpy(&slotNum, (char* )currPageBuffer + ixCurrKeyPtr + INT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                        ixCurrKeyPtr += INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                        std::cout << "Inside getNextEntry, in preparing int, pageNum is " << pageNum << " slotNum is " << slotNum <<" key is "<< *(int*)key<< std::endl;

                    }
                    else{
                        return IX_EOF;
                    }
                }
                else{
                    if (currKeyInt < highKeyInt){
                        memcpy((char* )key,  (char* )currPageBuffer + ixCurrKeyPtr, INT_SIZE);
                        memcpy(&pageNum, (char* )currPageBuffer + ixCurrKeyPtr + INT_SIZE, PTR_PN_SIZE);
                        memcpy(&slotNum, (char* )currPageBuffer + ixCurrKeyPtr + INT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                        ixCurrKeyPtr += INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    }
                    else{
                        return IX_EOF;
                    }
                }
            }
            else {
                float currKeyFlt;
                memcpy(&currKeyFlt, (char*) currPageBuffer+ixCurrKeyPtr, FLT_SIZE);
                float highKeyFlt;
                memcpy(&highKeyFlt, (char*) highKey, INT_SIZE);
                if (highKeyInclusive){
                    if (currKeyFlt <= highKeyFlt){
                        memcpy((char* )key,  (char* )currPageBuffer + ixCurrKeyPtr, FLT_SIZE);
                        memcpy(&pageNum, (char* )currPageBuffer + ixCurrKeyPtr + FLT_SIZE, PTR_PN_SIZE);
                        memcpy(&slotNum, (char* )currPageBuffer + ixCurrKeyPtr + FLT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                        ixCurrKeyPtr += FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    }
                    else{
                        return IX_EOF;
                    }
                }
                else{
                    if (currKeyFlt < highKeyFlt){
                        memcpy((char* )key,  (char* )currPageBuffer + ixCurrKeyPtr, FLT_SIZE);
                        memcpy(&pageNum, (char* )currPageBuffer + ixCurrKeyPtr + FLT_SIZE, PTR_PN_SIZE);
                        memcpy(&slotNum, (char* )currPageBuffer + ixCurrKeyPtr + FLT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                        ixCurrKeyPtr += FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                    }
                    else{
                        return IX_EOF;
                    }
                }
            }
        }
        //first time call getNextEntry
        else {
            std::cout << "Inside getNextEntry, entered else " << std::endl;
            int pageNumTobeScanned;
            unsigned short currKeyPtr;
            isFirstGetNextEntry = false;
            bool isLeaf = ix.getIsLeaf(currPageBuffer);
            while (!isLeaf) {
                currKeyPtr = IS_LEAF_SIZE + PTR_PN_SIZE;
                if (attrType == TypeVarChar) {
                    unsigned currVarCharLen;
                    std::string lowKeyVarChar = std::string((char*) lowKey+VC_LEN_SIZE, lowKeyLength-VC_LEN_SIZE);
                    for (int i = 0; i < numKeys; i++) {
                        memcpy(&currVarCharLen, (char *) currPageBuffer + currKeyPtr, VC_LEN_SIZE);
                        std::string currKeyVarChar = std::string((char *) currPageBuffer + currKeyPtr + VC_LEN_SIZE,currVarCharLen);
                        if (highKeyInclusive) {
                            if (lowKeyVarChar <= currKeyVarChar) break;
                        }
                        else {
                            if (lowKeyVarChar < currKeyVarChar) break;
                        }

                        unsigned short sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                        currKeyPtr += sizePassed;
                    }
                    memcpy(&pageNumTobeScanned, (char *)currPageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);
                    RC errCode = ixFileHandle->fileHandle.readPage(pageNumTobeScanned, currPageBuffer);
                    if (errCode != 0) return errCode;
                    isLeaf = ix.getIsLeaf(currPageBuffer);
                    numKeys = ix.getNumKeys(currPageBuffer);
                }
                else if (attrType == TypeInt) {
                    int lowKeyInt;
                    memcpy(&lowKeyInt, (char*) lowKey, INT_SIZE);
                    for (int i = 0; i < numKeys; i++) {
                        int currKeyInt;
                        memcpy(&currKeyInt, (char*) currPageBuffer+currKeyPtr, INT_SIZE);
                        if (highKeyInclusive) {
                            if (lowKeyInt <= currKeyInt) break;
                        }
                        else {
                            if (lowKeyInt < currKeyInt) break;
                        }

                        unsigned short sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                        currKeyPtr += sizePassed;
                    }
                    memcpy(&pageNumTobeScanned, (char *)currPageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);
                    RC errCode = ixFileHandle->fileHandle.readPage(pageNumTobeScanned, currPageBuffer);
                    if (errCode != 0) return errCode;
                    isLeaf = ix.getIsLeaf(currPageBuffer);
                    numKeys = ix.getNumKeys(currPageBuffer);
                }
                else {
                    float lowKeyFlt;
                    memcpy(&lowKeyFlt, (char*) lowKey, FLT_SIZE);
                    for (int i = 0; i < numKeys; i++) {
                        int currKeyFlt;
                        memcpy(&currKeyFlt, (char*) currPageBuffer+currKeyPtr, FLT_SIZE);
                        if (highKeyInclusive) {
                            if (lowKeyFlt <= currKeyFlt) break;
                        }
                        else {
                            if (lowKeyFlt < currKeyFlt) break;
                        }

                        unsigned short sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE+PTR_PN_SIZE;
                        currKeyPtr += sizePassed;
                    }
                    memcpy(&pageNumTobeScanned, (char *)currPageBuffer+currKeyPtr-PTR_PN_SIZE, PTR_PN_SIZE);
                    RC errCode = ixFileHandle->fileHandle.readPage(pageNumTobeScanned, currPageBuffer);
                    if (errCode != 0) return errCode;
                    isLeaf = ix.getIsLeaf(currPageBuffer);
                    numKeys = ix.getNumKeys(currPageBuffer);
                }
            }
            std::cout << "Inside getNextEntry, attrType is " << attrType << std::endl;
            //after while find corresponding leaf node
            currKeyPtr = IS_LEAF_SIZE;
            int keyIdx;
            if (attrType == TypeVarChar) {
                unsigned currVarCharLen;
                std::string lowKeyVarChar = std::string((char*) lowKey+VC_LEN_SIZE, lowKeyLength-VC_LEN_SIZE);
                for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                    memcpy(&currVarCharLen, (char *) currPageBuffer + currKeyPtr, VC_LEN_SIZE);
                    std::string currKeyVarChar = std::string((char *) currPageBuffer + currKeyPtr + VC_LEN_SIZE,currVarCharLen);
                    if (lowKeyInclusive){
                        if (lowKeyVarChar <= currKeyVarChar) break;
                    }
                    else {
                        if (lowKeyVarChar < currKeyVarChar)  break;
                    }
                    unsigned short sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                }
                if (keyIdx < numKeys){
                    memcpy((char* )key,  (char* )currPageBuffer + currKeyPtr, VC_LEN_SIZE + currVarCharLen);
                    memcpy(&pageNum, (char* )currPageBuffer + currKeyPtr + VC_LEN_SIZE + currVarCharLen, PTR_PN_SIZE);
                    memcpy(&slotNum, (char* )currPageBuffer + currKeyPtr + VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE, PTR_SN_SIZE);
                    ixCurrKeyPtr = currKeyPtr + VC_LEN_SIZE + currVarCharLen + PTR_PN_SIZE + PTR_SN_SIZE;;
                }
            }
            else if (attrType == TypeInt) {
                std::cout << "Inside getNextEntry, prepare output Int, lowKey is " << *(int*) this->lowKey << std::endl;
                //int lowKeyInt;
                //memcpy(&lowKeyInt, (char*) lowKey, INT_SIZE);
                int lowKeyInt = *(int*) this->lowKey;
                int currKeyInt;
                for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                    memcpy(&currKeyInt, (char*) currPageBuffer+currKeyPtr, INT_SIZE);
                    std::cout << "Inside getNextEntry, prepare output Int, lowKeyInt is " << lowKeyInt << " currKeyInt is " << currKeyInt << std::endl;
                    if (lowKeyInclusive){
                        if (lowKeyInt <= currKeyInt) break;
                    }
                    else {
                        if (lowKeyInt < currKeyInt)  break;
                    }
                    unsigned short sizePassed = INT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                }
                std::cout << "Inside getNextEntry, prepare output Int, after for loop " << std::endl;
                if (keyIdx < numKeys){
                    memcpy((char* )key,  (char* )currPageBuffer + currKeyPtr, INT_SIZE);
                    memcpy(&pageNum, (char* )currPageBuffer + currKeyPtr + INT_SIZE, PTR_PN_SIZE);
                    memcpy(&slotNum, (char* )currPageBuffer + currKeyPtr + INT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                    ixCurrKeyPtr = currKeyPtr + INT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;

                }
            }
            else {
                float lowKeyFlt;
                memcpy(&lowKeyFlt, (char*) lowKey, FLT_SIZE);
                float currKeyFlt;
                for (keyIdx = 0; keyIdx < numKeys; keyIdx++) {
                    memcpy(&currKeyFlt, (char*) currPageBuffer+currKeyPtr, FLT_SIZE);
                    if (lowKeyInclusive){
                        if (lowKeyFlt <= currKeyFlt) break;
                    }
                    else {
                        if (lowKeyFlt < currKeyFlt) break;
                    }
                    unsigned short sizePassed = FLT_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                    currKeyPtr += sizePassed;
                }
                if (keyIdx < numKeys){
                    memcpy((char* )key,  (char* )currPageBuffer + currKeyPtr, FLT_SIZE);
                    memcpy(&pageNum, (char* )currPageBuffer + currKeyPtr + FLT_SIZE, PTR_PN_SIZE);
                    memcpy(&slotNum, (char* )currPageBuffer + currKeyPtr + FLT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                    ixCurrKeyPtr = currKeyPtr + FLT_SIZE + PTR_PN_SIZE + PTR_SN_SIZE;
                }
            }
            //ixCurrPageNum = pageNumTobeScanned;
            std::cout << "Inside getNextEntry, before test keyIdx " << std::endl;
            if (keyIdx == numKeys){
                int nextPageNum = ix.getNextPageNum(currPageBuffer);
                if (nextPageNum == -1) return IX_EOF; // not found
                RC errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
                if (errCode != 0) return errCode;
                numKeys = ix.getNumKeys(currPageBuffer);
                while(numKeys == 0){
                    nextPageNum = ix.getNextPageNum(currPageBuffer);
                    if (nextPageNum == -1)  return IX_EOF;
                    RC errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
                    if (errCode != 0) return errCode;
                    numKeys = ix.getNumKeys(currPageBuffer);
                }
                if (attrType == TypeVarChar){
                    unsigned varCharlen;
                    memcpy(&varCharlen, (char* )currPageBuffer + IS_LEAF_SIZE, VC_LEN_SIZE);
                    memcpy(&pageNum, (char* )currPageBuffer + IS_LEAF_SIZE + VC_LEN_SIZE + varCharlen, PTR_PN_SIZE);
                    memcpy(&slotNum, (char* )currPageBuffer + IS_LEAF_SIZE + VC_LEN_SIZE + varCharlen + PTR_PN_SIZE, PTR_SN_SIZE);
                    memcpy((char* )key,  (char* )currPageBuffer + IS_LEAF_SIZE, VC_LEN_SIZE + varCharlen);
                    ixCurrKeyPtr = IS_LEAF_SIZE + VC_LEN_SIZE + varCharlen + PTR_SN_SIZE + PTR_PN_SIZE;
                }
                else{
                    memcpy(&pageNum, (char* )currPageBuffer + IS_LEAF_SIZE + INT_OR_FLT_SIZE, PTR_PN_SIZE);
                    memcpy(&slotNum, (char* )currPageBuffer + IS_LEAF_SIZE + INT_OR_FLT_SIZE + PTR_PN_SIZE, PTR_SN_SIZE);
                    memcpy((char* )key,  (char* )currPageBuffer + IS_LEAF_SIZE, INT_OR_FLT_SIZE);
                    ixCurrKeyPtr = IS_LEAF_SIZE + INT_OR_FLT_SIZE + PTR_SN_SIZE + PTR_PN_SIZE;
                }
                //ixCurrPageNum = nextPageNum;
                //errCode = ixFileHandle->fileHandle.readPage(nextPageNum, currPageBuffer);
                //if (errCode != 0) return errCode;
            }
        }
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        //std::cout << "Inside getNextEntry, before return 0, pageNum is " << pageNum << " slotNum is " << slotNum << std::endl;
        //free(currPageBuffer);
        return 0;
    }

    RC IX_ScanIterator::close() {
        free(currPageBuffer);
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        RC errCode = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
        if (errCode != 0) return errCode;

        ixReadPageCounter = readPageCount;
        ixWritePageCounter = writePageCount;
        ixAppendPageCounter = appendPageCount;
        return 0;
    }

} // namespace PeterDB