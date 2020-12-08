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



### 4. Project
- Describe how your project works.



### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)



### 6. Index Nested Loop Join
- Describe how your index nested loop join works. 



### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).

  Not implemented.

### 8. Aggregation
- Describe how your basic aggregation works.


- Describe how your group-based aggregation works. (If you have implemented this feature)
  
  

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
