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


RC BTreeIndex::insertHelper(const RecordId& rid, int key, PageId pid, int &new_key, PageId &new_pid, int curr_height)
{
	RC rc;

	if (curr_height < treeHeight)
	{
		BTNonLeafNode *nln = new BTNonLeafNode;
		rc = nln->read(pid, pf);
		if (rc < 0)
			return rc;

		PageId next_pid;
		rc = nln->locateChildPtr(key, next_pid);
		if (rc < 0)
			return rc;

		int new_key;
		PageId new_pid;
		rc = insertHelper(rid, key, next_pid, new_key, new_pid, curr_height+1);
		if (rc == RC_NODE_FULL) {
			rc = nln->insert(new_key, new_pid);

			if (rc == 0) {
				rc = nln->write(pid, pf);
				return rc;
			}
			else if (rc == RC_NODE_FULL) {
				int midKey;
				BTNonLeafNode *sib = new BTNonLeafNode;

				rc = nln->insertAndSplit(new_key, new_pid, *sib, midKey);
				if (rc < 0)
					return rc;
				new_pid = pf.endPid();

				rc = sib->write(new_pid, pf);
				rc = nln->write(pid, pf);
				if (rc < 0)
					return rc;

				new_key = midKey;
			}
			else return rc;

		}
		else return rc;
	}
	else if (curr_height == treeHeight) // leaf node
	{
		BTLeafNode *ln = new BTLeafNode;
		ln->read(pid, pf);

		rc = ln->insert(key, rid);
		if (rc == 0) {
			ln->write(pid, pf);
			return 0;
		}

		BTLeafNode *sib = new BTLeafNode;
		int sib_key;
		rc = ln->insertAndSplit(key, rid, *sib, sib_key);
		if (rc < 0)
			return rc;

		PageId sib_pid = pf.endPid();
		rc = sib->write(sib_pid, pf);
		if (rc < 0)
			return rc;

		ln->setNextNodePtr(sib_pid);
		rc = ln->write(pid, pf);
		if (rc < 0)
			return rc;

		return 0;

	}

	//rc = insert(key, cursor.pid); //nonleafnode
	//if (rc < 0)
	//	return rc;
	//insertAndSplit;
	//locate;
}


/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	RC rc;
	// If there are no nodes in the tree
	if (treeHeight == 0) {
		BTLeafNode *ln = new BTLeafNode;
		rc = ln->insert(key, rid);
		if (rc < 0)
			return rc;
	
		rootPid = 1;
		rc = ln->write(rootPid, pf);
		if (rc < 0)
			return rc;

		treeHeight = 1;
	}
	else {
		int new_key;
		PageId new_pid;
		rc = insertHelper(rid, key, rootPid, new_key, new_pid, 1);

		// If the node had to be split then should initialize a new root
		if (rc == RC_NODE_FULL)
		{
			BTNonLeafNode *new_root = new BTNonLeafNode;
			rc = new_root->initializeRoot(rootPid, new_key, new_pid);
			if (rc < 0)
				return rc;

			rootPid = pf.endPid();
			rc = new_root->write(rootPid, pf);
			if (rc < 0)
				return rc;

			treeHeight++;

			return 0;
		}

		
	}

	/*
	IndexCursor cursor;
	RC rc;
	rc = locate(key, cursor);
	if (rc < 0)
		return rc;

	insertHelper(key, rid, cursor.pid, int &new_key, PageId &new_pid, int curr_height)


	BTLeafNode ln;
	//rc = *(cursor.pid)->insert(key, rid); //leafnode
	if (rc == 0)
		return 0;
	int sib_key;
	BTLeafNode sib_ln;
	ln.insertAndSplit(key, rid, sib_ln, sib_key);
	//get midkey

	int counter = 1;
	locateHelper(key, cursor, 0, treeHeight-counter);
	while (rc != 0 && counter <= treeHeight) {
		insert(key, cursor.pid); //nonleafnode
		//insertsplit;
		//locate;
	}
	*/

    return 0;
}

RC BTreeIndex::locateHelper(int searchKey, IndexCursor& cursor, int counter, int treeHeight) {
	RC rc;
	if (counter == treeHeight) { // leaf node
		BTLeafNode *ln = new BTLeafNode();
		rc = ln->locate(searchKey, cursor.eid);
		if (rc < 0)
			return rc;
	}
	else { // nonleaf node
		BTNonLeafNode *n = new BTNonLeafNode();
		rc = n->locateChildPtr(searchKey,cursor.pid);
		if (rc < 0)
			return rc;
		return locateHelper(searchKey, cursor, counter+1, treeHeight);
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
	/*if(cursor.pid < 0 || cursor.pid >= pf.endPid())
    	return RC_INVALID_CURSOR;*/

	RC rc;
	rc = locateHelper(searchKey, cursor, 0, treeHeight); // puts the cursor at the leaf node
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
