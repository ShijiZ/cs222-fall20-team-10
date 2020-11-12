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
        RC errCode = pfm->createFile(fileName);
        if (errCode != 0) return errCode;

        FileHandle fileHandle;
        errCode = pfm->openFile(fileName, fileHandle);
        if (errCode != 0) return errCode;

        void* metaDataPage = malloc(PAGE_SIZE);
        unsigned rootPageNum = 0;
        memcpy((char*) metaDataPage, &rootPageNum, sizeof(unsigned));
        fileHandle.appendPage(metaDataPage);
        free(metaDataPage);

        return pfm->closeFile(fileHandle);
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

        // B+ tree has 0 node
        if (ixFileHandle.fileHandle.getNumberOfPages() == 1) {
            initLeafNode(pageBuffer);
            ixFileHandle.fileHandle.appendPage(pageBuffer);

            unsigned rootPageNum = 1;
            ixFileHandle.fileHandle.readPage(0, pageBuffer);
            memcpy((char*) pageBuffer, &rootPageNum, sizeof(unsigned));
            ixFileHandle.fileHandle.writePage(0, pageBuffer);
            return 0;
        }
        // B+ tree has at least 1 node
        else {
            unsigned rootPageNum;
            ixFileHandle.fileHandle.readPage(0, pageBuffer);
            memcpy(&rootPageNum, (char*) pageBuffer, sizeof(unsigned));

            ixFileHandle.fileHandle.readPage(rootPageNum, pageBuffer);
            unsigned newChildPageNum = 0;

            AttrType attrType = attribute.type;
            unsigned keyLength;
            if (attrType == TypeVarChar) {
                memcpy(&keyLength, key, VC_LEN_SIZE);
                keyLength += VC_LEN_SIZE;
            }
            else if (attrType == TypeInt){
                keyLength = INT_SIZE;
            }
            else {
                keyLength = FLT_SIZE;
            }

            RC errCode = insertEntry(ixFileHandle, pageBuffer, rootPageNum, keyLength, attrType, key, rid, newChildPageNum);
            if (errCode != 0) return errCode;
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
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    /**********************************/
    /*****    Helper functions  *******/
    /**********************************/
    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, void* pageBuffer, unsigned pageNum, unsigned keyLength,
                                 AttrType attrType, const void *key, const RID &rid, unsigned& newChildPageNum) {
        bool isLeaf;
        memcpy(&isLeaf, pageBuffer, IS_LEAF_SIZE);

        if (isLeaf) {
            unsigned short freeBytes = getFreeBytes(pageBuffer);
            unsigned short numKeys = getNumKeys(pageBuffer);
            unsigned bytesNeeded = keyLength + PTR_PN_SIZE + PTR_SN_SIZE;
            // new key can be inserted in this leaf node
            if (bytesNeeded <= freeBytes) {
                if (attrType == TypeVarChar) {
                    std::string newKeyVarChar = std::string((char*) key+VC_LEN_SIZE, keyLength-VC_LEN_SIZE);
                    unsigned short currKeyPtr = IS_LEAF_SIZE;
                    unsigned currVarCharLen;
                    unsigned short sizeToBeShifted = PAGE_SIZE-F_SIZE-N_SIZE-freeBytes-IS_LEAF_SIZE;
                    for (int i = 0; i < numKeys; i++) {
                        memcpy(&currVarCharLen, (char*) pageBuffer+currKeyPtr, VC_LEN_SIZE);
                        std::string currKeyVarChar = std::string((char*) pageBuffer+currKeyPtr+VC_LEN_SIZE, currVarCharLen);

                        bool insertHere = false;
                        if (newKeyVarChar == currKeyVarChar) {
                            unsigned currPageNum;
                            memcpy(&currPageNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen, PTR_PN_SIZE);
                            if (rid.pageNum == currPageNum) {
                                unsigned currSlotNum;
                                memcpy(&currSlotNum, (char*) pageBuffer+currKeyPtr+VC_LEN_SIZE+currVarCharLen+PTR_PN_SIZE, PTR_SN_SIZE);
                                // Equal slotNum is impossible
                                if (rid.slotNum < currPageNum) insertHere = true;
                            }
                            else if (rid.pageNum < currPageNum) insertHere = true;
                        }
                        else if (newKeyVarChar < currKeyVarChar) insertHere = true;

                        if (insertHere) {
                            shiftKey(pageBuffer, currKeyPtr, sizeToBeShifted, bytesNeeded);
                            memcpy((char*) pageBuffer+currKeyPtr, (char*) key, keyLength);
                            memcpy((char*) pageBuffer+currKeyPtr+keyLength, &rid.pageNum, PTR_PN_SIZE);
                            memcpy((char*) pageBuffer+currKeyPtr+keyLength+PTR_PN_SIZE, &rid.slotNum, PTR_SN_SIZE);
                            ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
                            return 0;
                        }
                        else {
                            unsigned short sizePassed = currVarCharLen+VC_LEN_SIZE+PTR_PN_SIZE+PTR_SN_SIZE;
                            sizeToBeShifted -= sizePassed;
                            currKeyPtr += sizePassed;
                        }
                    }
                }
                else if (attrType == TypeInt) {

                }
                else {

                }
            }
            // new key cannot be inserted in this leaf node, must split
            else {

            }
        }
        // node is non-leaf node
        else {

        }
    }

    void IndexManager::shiftKey(void* pageBuffer, short keyOffset, short sizeToBeShifted, short distance) {
        memcpy((char*) pageBuffer+keyOffset+distance, (char*) pageBuffer+keyOffset, sizeToBeShifted);
    }


    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
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