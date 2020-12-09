## Project 4 Report


### 1. Basic information
 - Team #: 10
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-10
 - Student 1 UCI NetID: shijiz
 - Student 1 Name: Shiji Zhao
 - Student 2 UCI NetID (if applicable): dahaih
 - Student 2 Name (if applicable): Dahai Hao


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns). 

- Tables table:

  Tables table has three columns: tableId, table_name and file_name. The format is shown below:

  | tableId |   table_name  |   file_name  |
  | :-----: | :-----------  | :----------  |
  |    1    |     Tables    |    Tables    |
  |    2    |     Columns   |    Columns   |
  |    3    |   Other_table |  Other_table |
  
- Columns table (Including information about indexes):
  
  Columns table has six columns: tableId, column_name, column_type, column_length, column_position, and column_indexed (int).
  The format is shown below (suppose there is another table with 3 attributes, and an index on attr_2_name was created):
  
  | tableId |   column_name   | column_type | column_length | column_position | column_indexed |
  | :-----: | :-------------- | :---------- | :------------ | :-------------- | :------------- |
  |    1    |     tableId     |     Int     |       4       |        1        |        0       |
  |    1    |    table_name   |   VarChar   |       50      |        2        |        0       |
  |    1    |    file_name    |   VarChar   |       50      |        3        |        0       |
  |    2    |     tableId     |     Int     |       4       |        1        |        0       |
  |    2    |   column_name   |   VarChar   |       50      |        2        |        0       |
  |    2    |   column_type   |     Int     |       4       |        3        |        0       |
  |    2    |  column_length  |     Int     |       4       |        4        |        0       |
  |    2    | column_position |     Int     |       4       |        5        |        0       |
  |    3    |   attr_1_name   | attr_1_type | attr_1_length |        1        |        0       |
  |    3    |   attr_2_name   | attr_2_type | attr_2_length |        2        |        1       |
  |    3    |   attr_3_name   | attr_3_type | attr_3_length |        3        |        0       |
  
  Note:
  1. We always assign tableId = 1 to Tables table and tableId = 2 to Columns table. 
  2. For the column_indexed column in the Columns table, 1 indicates an index of this attribute exists; 0 indicate index of this attribute doesn't exist.

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

  We iterate through the table and every time we get a tuple, we extract the attribute value to be compared, compare it with filter's compare key. If it satisfies the filter condition, we output this tuple.


### 4. Project
- Describe how your project works.

  We iterate through the table and every time we get a tuple, we extract the attributes to be projected, generate a new tuple with these attributes, then out put the new tuple.


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

  1. Initialize a memory block of size numPages * PAGE_SIZE. 
  
  2. Iterate through the left table, load the left tuples into the block until the block is full or the left scan is completed. After the block is prepared, a separated hash table is built based on the join key. 
  
  3. Iterate through the right table and get tuples from the right table. For every tuple from the right table, probe the hash table and get all the matched left tuples in the memory block and output all these matched (left tuple, right tuple) pairs. After the right scan is completed, if the left scan is not completed, reset the right iterator, clear the hash table and go to step ii. Otherwise, return QE_EOF.
   


### 6. Index Nested Loop Join
- Describe how your index nested loop join works. 

  1. Iterate through the left table, get one left tuple from the left table and extract the attribute to be joined. Call the attribute value compare key.
  
  2. Iterate through the right table with a index scan iterator, do a equal search based on the compare key to get all the matched right tuples. Output all these (left tuple, right tuple) pairs. If the left scan is not completed, go to step i. Otherwise, return QE_EOF.


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).

  Not implemented.

### 8. Aggregation
- Describe how your basic aggregation works.

  We iterate through the table and every time we get a tuple, we extract the attribute to be aggregated, maintain the running information in according to the aggregation operation. After the iteration is completed, return the running information.


- Describe how your group-based aggregation works. (If you have implemented this feature)

  For group-based aggregation, after completing iterating through the table, we build a hash table using std::unordered_map<key, value>, where a key is a attribute value of the groupAttr, and the mapped value is a vector of the aggAttr values in that group.
  After the hash table is built, we calculate the running information of each group and return these running information.
  
  

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

  No.

- Other implementation details:

  The demo video of the extra credit Texera project can be found [here](https://www.youtube.com/watch?v=gh8xmvQexqY) (With Audio), or copy the following URL: https://www.youtube.com/watch?v=gh8xmvQexqY


### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.

  Shiji and Dahai contributed equally to this project. We work and debug together for most of the time.
  

### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
