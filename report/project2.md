## Project 2 Report


### 1. Basic information
 - Team #: 10
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-10
 - Student 1 UCI NetID: shijiz
 - Student 1 Name: Shiji Zhao
 - Student 2 UCI NetID (if applicable): dahaih
 - Student 2 Name (if applicable): Dahai Hao


### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

- Tables table:

  Tables table has three columns: tableId, table_name and file_name. The format is shown below:
  
  | tableId |   table_name  |   file_name  |
  | :-----: | :-----------  | :----------  |
  |    1    |     Tables    |    Tables    |
  |    2    |     Columns   |    Columns   |
  |    3    |   Other_table |  Other_table |
  
- Columns table:
  
  Columns table has five columns: tableId, column_name, column_type, column_length and column_position.
  The format is shown below (suppose there is another table with 3 attributes):
  
  | tableId |   column_name   | column_type | column_length | column_position |
  | :-----: | :-------------- | :---------- | :------------ | :-------------- |
  |    1    |     tableId     |     Int     |       4       |        1        |
  |    1    |    table_name   |   VarChar   |       50      |        2        |
  |    1    |    file_name    |   VarChar   |       50      |        3        |
  |    2    |     tableId     |     Int     |       4       |        1        |
  |    2    |   column_name   |   VarChar   |       50      |        2        |
  |    2    |   column_type   |     Int     |       4       |        3        |
  |    2    |  column_length  |     Int     |       4       |        4        |
  |    2    | column_position |     Int     |       4       |        5        |
  |    3    |   attr_1_name   | attr_1_type | attr_1_length |        1        |
  |    3    |   attr_2_name   | attr_2_type | attr_2_length |        2        |
  |    3    |   attr_3_name   | attr_3_type | attr_3_length |        3        |

  Note that we always assign tableId = 1 to Tables table and tableId = 2 to Columns table. 


### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

  Our on-page record has the following format:
  
  attrNum | attrOffsetDir | attrSection
  - attrNum: an unsigned value storing the total number of attributes in the record (including NULL).
  - attrOffsetDir: attrNum of int values storing the beginning (offset) of corresponding attributes (including NULL) relative to the beginning of attrSection.
  - attrSection: each formatted attribute except for NULL attributes are stored here.
  
  If the record is moved to somewhere else due to update, then we put a 6-byte pointer here as a record which consists of a page number as an int and a slot number as a short.

- Describe how you store a null field.

  For NULL attribute, its corresponding int value in attrOffsetDir is -1, and it uses no space in attrSection.


- Describe how you store a VarChar field.

  For VarChar attribute, we store its length as an unsigned value, then its content is stored after its length.


- Describe how your record design satisfies O(1) field access.

  We first find the corresponding int value in the attrOffsetDir. If it is -1, the attribute is NULL. Otherwise, access its beginning according to its offset. If it is of integer or real types, read 4 bytes. If it is of varChar type, read 4 bytes to get its length, then read its content according to its length.


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

  Our page has the following format:
  
  recordsSection | freeBytesSection | slotsDirectory | numSlots | FreeBytes
  - recordsSection: Each record is stored from left to right.
  - freeBytesSection: Free bytes in the page.
  - slotsDirectory: Two shorts for each record stored from right to left. (details explained below)
  - numSlots: An unsigned short value storing the total number of slots stored in the page.
  - FreeBytes: An unsigned short value storing the total free bytes in the page.

- Explain your slot directory design if applicable.

  Each slot for corresponding record has the following format:
    
  recordOffset | recordLength
      
  - recordOffset: A short storing the beginning (offset) of corresponding record relative to the beginning of the page. If the record has been deleted, we set it to be -1.
  - recordLength: A short storing the length of corresponding record. If the record have been moved to another page, we set it to be -1.
  

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

  One.

- Show your hidden page(s) format design if applicable

  Four unsigned values are stored consecutively at the beginning of the hidden page. They are:
  
  - readPageCounter
  - writePageCounter
  - appendPageCounter
  - numPages


### 6. Describe the following operation logic.
- Delete a record

1. Find the actual location (pageNum and slotNum) of the record and get its recordOffset and recordLength.

2. Find all the records in this page whose record offset is greater than recordOffset, shift all these records left by recordLength. 

3. In the slotsDirectory, set the record offset of all the shifted records to be their original offsets minus recordLength.

4. In the slotsDirectory, set recordOffset of the deleted record to be -1. 

- Update a record

1. Find the actual location (pageNum and slotNum) of the record and get its recordOffset and recordLength. Get the newRecordLength of the record.

2. If there are enough free space for this record in this page,

   - Find all the records in this page whose record offset is greater than recordOffset, and shift all these records by distance = newRecordLength - recordLength ( distance > 0 means shift right, < 0 means shift left ).
   - In the slotsDirectory, set the record offset of all the shifted records to be their original offsets plus distance.
   - Put the updated record at the original location.
   - In the slotsDirectory, set the record length of the updated record to be newRecordLength.
   
   If there are not enough free space for this record in the page,
   
   - Insert this record to a new page using similar logic of insertRecord().
   - Find all the records in the original page whose record offset is greater than recordOffset, and shift all these records by distance = 6 - recordLength (a negative number, thus to the left).
   - In the slotsDirectory, set the record offset of all the shifted records to be their original offsets plus distance.
   - At the original location of the updated record, put a 6-byte pointer (pageNum, slotNum) which points to its new location (pageNum and slotNum). 
   - In the slotsDirectory of the original page, set the record length of the 6-byte pointer to be -1.
   

- Scan on normal records

  Starting from current page number and slot number, get the corresponding record, and check if it meets the condition. If it does, output the desired attributes with a null indicator. 
 
- Scan on deleted records

  If the record has been deleted, just skip it and continue scanning the next record.

- Scan on updated records

  If the record is still on the original location (pageNum and slotNum), treat it as a normal record. If it has been moved to another page, just skip it on the current page and continue scanning the next record.


### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

  Shiji and Dahai contributed equally to this project. We work and debug together for most of the time.


### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)