#ifndef _ix_h_
#define _ix_h_

#define ROOT_PAGE_NUM_SIZE sizeof(unsigned)
#define IS_LEAF_SIZE sizeof(bool)
#define NXT_PN_SIZE sizeof(int)
#define ORDER 2043

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        unsigned getRootPageNum(IXFileHandle &ixFileHandle) const;

        bool getIsLeaf(void* pageBuffer) const;

        unsigned short getNumKeys(void* pageBuffer) const;

    //private:
        PagedFileManager* pfm;

        /**********************************/
        /*****    Helper functions  *******/
        /**********************************/

        RC insertEntry1(IXFileHandle &ixFileHandle, void* pageBuffer, unsigned pageNum, unsigned keyLength,
                       AttrType attrType, const void *key, const RID &rid, void* newChildEntry, unsigned rootPageNum);

        RC insertEntry2(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                        unsigned short bytesNeeded, unsigned short freeBytes, unsigned short numKeys,
                        AttrType attrType, bool isLeaf);

        void shiftKey(void* pageBuffer, short keyOffset, short sizeToBeShifted, unsigned short distance);

        RC insertEntry3(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                        short currKeyPtr, short sizeToBeShifted, unsigned short bytesNeeded, bool isLeaf);


        RC findPageNumToBeHandled(void* pageBuffer, unsigned keyLength, const void *key, const RID &rid,
                                  unsigned short numKeys, AttrType attrType, int& pageNumToBeInserted);

        RC splitNode(IXFileHandle &ixFileHandle, void* pageBuffer, unsigned keyLength,
                     AttrType attrType, const void *key, const RID &rid, void* newChildEntry,
                     unsigned pageNum, unsigned short bytesNeeded,unsigned short freeBytes,
                     unsigned short numKeys, bool isLeaf, bool isRoot);

        RC initLeafNode(void* pageBuffer, void* entryPtr, unsigned short bytesNeeded,
                        int numKeys, int nextPageNum);

        RC initNonLeafNode(void* pageBuffer, void* entryPtr, unsigned short bytesNeeded, int numKeys);

        RC printNode(IXFileHandle &ixFileHandle, AttrType attrType, unsigned pageNum,
                     int indent, std::ostream &out) const;

        /*********************************************/
        /*****    Getter and Setter functions  *******/
        /*********************************************/



        unsigned short getFreeBytes(void* pageBuffer) const;

        int getNextPageNum(void* pageBuffer) const;

        void setIsLeaf(void* pageBuffer, bool isLeaf);

        void setNumKeys(void* pageBuffer, unsigned short numKeys);

        void setFreeBytes(void* pageBuffer, unsigned short freeBytes);

        void setNextPageNum(void* pageBuffer, int nextPageNum);

    protected:
        IndexManager();                                                             // Prevent construction

        ~IndexManager();                                                            // Prevent unwanted destruction

        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying

        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator() ;

        RC initialize(IXFileHandle &ixFileHandle, const Attribute &attribute,
                      const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        IXFileHandle *ixFileHandle;

    private:
        AttrType attrType;
        const void* lowKey;
        const void* highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        //int ixCurrPageNum;
        int ixCurrKeyPtr;
        bool isFirstGetNextEntry;
        void* currPageBuffer;

        int minInt = std::numeric_limits<int>::min();
        int maxInt = std::numeric_limits<int>::max();
        float minFlt = std::numeric_limits<float>::min();
        float maxFlt = std::numeric_limits<float>::max();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        FileHandle fileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };
}// namespace PeterDB
#endif // _ix_h_
