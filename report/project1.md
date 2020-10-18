## Project 1 Report


### 1. Basic information
 - Team #: 10
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-10
 - Student 1 UCI NetID: shijiz
 - Student 1 Name: Shiji Zhao
 - Student 2 UCI NetID (if applicable): dahaih
 - Student 2 Name (if applicable): Dahai Hao


### 2. Internal Record Format
- Show your record format design.

  Our on-page record has the following format:
  
  attrNum | attrOffsetDir | attrSection
  - attrNum: an unsigned value storing the total number of attributes in the record (including NULL).
  - attrOffsetDir: attrNum of int values storing the beginning (offset) of corresponding attributes (including NULL) relative to the beginning of attrSection.
  - attrSection: each formatted attribute except for NULL attributes are stored here.
  
- Describe how you store a null field.

  For NULL attribute, its corresponding int value in attrOffsetDir is -1, and it use no space in attrSection.

- Describe how you store a VarChar field.

  For VarChar attribute, we store its length as an unsigned value, then its content is stored after its length.


- Describe how your record design satisfies O(1) field access.

  We first find the corresponding int value in the attrOffsetDir. If it is -1, the attribute is NULL. Otherwise, access its beginning according to its offset. If it is of integer or real types, read 4 bytes. If it is of varChar type, read 4 bytes to get its length, then read its content according to its length.

### 3. Page Format
- Show your page format design.

  Our page has the following format:
  
  recordsSection | freeBytesSection | slotsDirectory | numSlots | FreeBytes
  - recordsSection: Each record is stored from left to right.
  - freeBytesSection: Free bytes in the page.
  - slotsDirectory: Two unsigned short for each record stored from right to left. (details explained below)
  - numSlots: An unsigned short value storing the total number of slots stored in the page.
  - FreeBytes: An unsigned short value storing the total free bytes in the page.
   
- Explain your slot directory design if applicable.

  Each slot for corresponding record has the following format:
    
  offset | length
      
  - offset: An unsigned short storing the beginning (offset) of corresponding record relative to the beginning of the page.
  - length: An unsigned short storing the length of corresponding record.


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

  The pseudocode is as follows:
  ```
  // determine bytes needed
  bytesNeeded = getBytesNeeded(record);
  recordInserted = false;
  
  // detect if page number is 0
  if (numPages > 0) {
    freeBytes = getFreeBytes(lastPage);
  
    // insert to the last page
    if (freeBytes >= bytesNeeded) {
      insertRecordToPage(record, lastPage);
      recordInserted = true;
    }
    // scan from the first page, inser if find capable one
    else {
      for (i=0; i<numPages-1; i++) {
        freeBytes = getFreeBytes(i);
        if (freeBytes >= bytesNeeded) {
          insertRecordToPage(record, i);
          recordInserted = true;
          break;
        }
      }
    }
  }
  
  // have to initialize new page and insert
  if (!recordInserted) {
    initNewPage();
    numPages++;
  }
  
  ```

- How many hidden pages are utilized in your design?

  One.

- Show your hidden page(s) format design if applicable

  Four unsigned values are stored consecutively at the beginning of the hidden page. They are:
  
  - readPageCounter
  - writePageCounter
  - appendPageCounter
  - numPages

### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.

  Shiji and Dahai contributed equally to this project. We work and debug together for most of the time.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)