## Project 3 Report


### 1. Basic information
 - Team #: 10
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-10
 - Student 1 UCI NetID: shijiz
 - Student 1 Name: Shiji Zhao
 - Student 2 UCI NetID (if applicable): dahaih
 - Student 2 Name (if applicable): Dahai Hao


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 

  In our design, we have one meta-data page with page number 0 which stores the page number of the root page.


### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
  
  For internal nodes, our design for one entry is shown below:
  
  key | pageNum | slotNum | pointer to right child node 
  - key: key is the actual data. If it is TypeInt or TypeReal, it occupies 4 bytes. If it is TypeVarChar, it uses 4 bytes for the length followed by the characters.
  - pageNum: heap file page number of the key.
  - slotNum: heap file slot number of the key.
  - pointer to right child node: index file page number of the right child node of the key.
  
  - entries on leaf nodes:
  
  For leaf nodes, our design for one entry is shown below:
  
  key | pageNum | slotNum
  - key: key is the actual data. If it is TypeInt or TypeReal, it occupies 4 bytes. If it is TypeVarChar, it uses 4 bytes for the length followed by the characters.
  - pageNum: heap file page number of the key.
  - slotNum: heap file slot number of the key.
  

### 4. Page Format
- Show your internal-page (non-leaf node) design.

  For internal-page, our design is shown below:
  
  IsLeaf | pointerZero | index entry 1 | index entry 2 | ...... | index entry N | free space | numKeys | freeBytes
  - IsLeaf: boolean variable indicates whether this is a leaf node or not.
  - pointerZero: index file page number of the left most child of this page. 
  - index entry 1-N: index entries in the internal page.
  - free space: free space of the page.
  - numKeys: an unsigned short value storing the number of entries in this page.
  - freeBytes: an unsigned short value storing the number of free bytes in this page.

- Show your leaf-page (leaf node) design.

  IsLeaf | data entry 1 | data entry 2 | ...... | data entry N | free space | nextPageNum | numKeys | freeBytes
  - IsLeaf: a boolean variable indicating whether this is a leaf node or not. 
  - data entry 1-N: data entries in the leaf page.
  - free space: free space of the page.
  - nextPageNum: an integer value storing the index file page number of the leaf page immediately on the right of this page. If this page is already the right most leaf page, we set nextPageNum to be -1.
  - numKeys: an unsigned short value storing the number of entries in this page.
  - freeBytes: an unsigned short value storing the number of free bytes in this page.


### 5. Describe the following operation logic.
- Split

  - If a leaf node needs to be split, first find the data entry which resides in the middle of the page, call it newChildEntry. Data entries before newChildEntry stay in the current page. Initialize a new leaf page and put the newChildEntry and (copy up) the following data entries in this new leaf page. Set the nextPageNum of the new leaf page to be the nextPageNum of the original page, and set the original page's nextPageNum to be the page number of the new leaf page. If the split node is a root node, initialize a new root page with pointerZero to be the page number of the split node, insert (newChildEntry, page number of the new leaf page) into this new root page. Otherwise, insert (newChildEntry, page number of the new leaf page) into the parent page.

  - If a non-leaf node needs to be split, first find the index entry which resides in the middle of the page, call it newChildEntry. Index entries before newChildEntry stay in the current page. Initialize a new non-leaf page and put the index entries after (push up) newChildEntry in this new non-leaf page. Reset the pointer to child node section of the newChildEntry to be the page number of the new non-leaf page. If the split node is a root node, initialize a new root page with pointerZero to be the page number of the split node, insert newChildEntry into this new root page. Otherwise, insert newChildEntry into the parent page.

  
- Rotation (if applicable)

  We did not implement rotation.

- Merge/non-lazy deletion (if applicable)

  We implemented lazy deletion


- Duplicate key span in a page

  We store each key together with its rid even in non-leaf pages. So in our design, key is actually (key, rid.pageNum, rid.slotNum). Because there can not be duplicate rids, there will be no duplicate keys in our design.

- Duplicate key span multiple pages (if applicable)

  We store each key together with its rid even in non-leaf pages. So in our design, key is actually (key, rid.pageNum, rid.slotNum). Because there can not be duplicate rids, there will be no duplicate keys in our design.


### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

  No.

- Other implementation details:



### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.

  Shiji and Dahai contributed equally to this project. We work and debug together for most of the time.


### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
