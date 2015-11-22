/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc;
	pf = PageFile(indexname, mode);
	rc = pf.open(indexname, mode);
	if (rc < 0) {
		return rc;
	}

	char buf[PageFile::PAGE_SIZE];

	if (pf.endPid() == 0) {
 		rootPid = -1;
 		treeHeight = 0;
	 	int* bufPtr = (int*) buf;
		bufPtr[0] = rootPid;
		bufPtr[1] = treeHeight;
		pf.write(0, buf);
	}
	else {
	 	pf.read(0, buf);
	 	int* bufPtr = (int*) buf;
	 	rootPid = bufPtr[0];
	 	treeHeight = bufPtr[1];
	}

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	RC rc;
	rc = pf.close();
	if (rc < 0) {
    	return rc;
 	}

    return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    return 0;
}

RC BTreeIndex::locateHelper(int searchKey, IndexCursor& cursor, int counter) {
	RC rc;
	if (counter = treeHeight) { // leaf node
		BTLeafNode *ln = new BTLeafNode();
		rc = ln->locate(searchKey, cursor.eid);
		if (rc < 0)
			return rc;
	}
	else { // nonleaf node
		rc = locateChildPtr(searchKey,cursor.pid);
		BTNonLeafNode *n = new BTNonLeafNode();
		if (rc < 0)
			return rc;
		return locateHelper(searchKey, cursor, counter+1);
	}
	return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	if(cursor.pid < 0 || cursor.pid >= pf.endPid())
    	return RC_INVALID_CURSOR;

	RC rc;
	rc = locateHelper(searchKey, cursor, 0); // puts the cursor at the leaf node
	if (rc < 0)
		return rc;

    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    if (cursor.pid < 0 || cursor.pid >= pf.endPid())
    	return RC_INVALID_CURSOR;

    //create a new BTLeafNode
    BTLeafNode * ln = new BTLeafNode();
    RC rc;

    if((rc = ln->read(cursor.pid, pf)) != 0)
    	return rc;
    if((rc = ln->readLEntry(cursor.eid, key, rid)) != 0)
    	return rc;

    if(cursor.eid == ln->getKeyCount()-1)
    {
    	cursor.pid = ln->getNextNodePtr();
    	cursor.eid = 0;
    }
    else
    	cursor.eid++;

    return 0;

}
