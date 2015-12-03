
For Part A:
We followed the implementation spec and used fstream to open the file and
then parsed each line in the file (depending on whether it was an empty
string or not). We added checks to make sure we were able to open the file
as well as the RecordFile. We also added checks for if there were errors
parsing the lines or appending tuples to the table.

For Part B:
We attempted to create a B+Tree for the non-leaf nodes using a struct called non_leafNodeEntry.

For Part C:
We implemented the BTreeIndex.

For Part D:
We implemented the load() and select() functions, which umltimately enabled querying functionality using the B+Tree. Our implementation allows users to search for specific keys/values or keys/values that fall within user-specified constraints.

Sharon Grewal (sharonkg08@gmail.com)
Kelly Ou (kellio94@ucla.edu)

