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
    //set the rootPid to -1
    rootPid = -1;
    
    //set treeHeight to 0
    treeHeight = 0;
}

BTreeIndex::BTreeIndex(const string& indexname, char mode)
{
	treeHeight = 0;
	pf = PageFile(indexname, mode);
	
	char buf[PageFile::PAGE_SIZE];
	bzero(buf, PageFile::PAGE_SIZE);
	int * b_ptr = (int*) buf;

	if(mode == 'r')
	{
		pf.read(0, buf);
		rootPid = *b_ptr;
		treeHeight = b_ptr[1];
	}
	else{
		if(pf.endPid() == 0)
		{	
			rootPid = 1;
			*b_ptr = rootPid;
			*(b_ptr+1) = treeHeight;
			pf.write(0, buf);
			BTLeafNode* root = new BTLeafNode();
			root->setNextNodePtr(-1);
			root->write(rootPid, pf);
			
		}
		else{
			pf.read(0, buf);
			rootPid = *b_ptr;
			treeHeight = b_ptr[1];
		}
	}
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
    //create an instance of PageFile
//	pf = PageFile(indexname, mode);


    //open the PageFile
	rc = pf.open(indexname, mode);

    //check if it opened
	if (rc < 0) {
		return rc; 
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
    //close the pageFile
	rc = pf.close();
	if (rc < 0) {
    	return rc; //check if it closed properly
 	}

    return 0;
}


RC BTreeIndex::insertHelper(const RecordId& rid, int key, PageId pid, int &new_key, PageId &new_pid, int curr_height)
{
	RC rc;

    //check if nonleaf node
	if (curr_height < treeHeight) 
	{
        //create an instance of BTNonLeafNode
		BTNonLeafNode *nln = new BTNonLeafNode;

        //read to pageFile
		rc = nln->read(pid, pf);
		if (rc < 0)
			return rc; 
        //check if read succeeded

        //locate the child of that non-leaf node
		PageId next_pid;
		rc = nln->locateChildPtr(key, next_pid);

        //check if locateChildPtr returned something
		if (rc < 0)
			return rc; 

		int new_key;
		PageId new_pid;

        //use insertHelper
		rc = insertHelper(rid, key, next_pid, new_key, new_pid, curr_height+1);

        //check if the node is full 
        //if full --> use insertAnd Split
        //otherwise just write to the pagefile after inserting
        if (rc == RC_NODE_FULL) {
            //insert the new key and pid
			rc = nln->insert(new_key, new_pid);

			if (rc == 0) {
                //if insert is successful
                //write the node into pagefile
				rc = nln->write(pid, pf);

				return rc;
			}
			else if (rc == RC_NODE_FULL) {
				int midKey;
                //create a new nonleaf node
                //this will be used for sibling
				BTNonLeafNode *sib = new BTNonLeafNode;

                //inser the new key & pid, sibling, midKey
				rc = nln->insertAndSplit(new_key, new_pid, *sib, midKey);

                //if insert&split is successful
				if (rc < 0) 
					return rc;

                //set new_pid to the last pid
				new_pid = pf.endPid();

                //write the sibling into pagefile
				rc = sib->write(new_pid, pf);

                //write the non leaf node to pagefile
				rc = nln->write(pid, pf);

                //if the write is successful
				if (rc < 0)
					return rc;

                    //set the new key to midKey
				new_key = midKey;
			}
			else return rc;

		}
		else return rc;
	}
	else if (curr_height == treeHeight) // leaf node
	{
        //create new leafNode
		BTLeafNode *ln = new BTLeafNode;

        //read this node
		ln->read(pid, pf);

        //insert the key&rid
		rc = ln->insert(key, rid);

		if (rc == 0) {
            //if the insert is successful
            //then write
			ln->write(pid, pf);
			return 0;
		}

            //otherwise create a sibling
		BTLeafNode *sib = new BTLeafNode;
		int sib_key;

        //use insert&split
		rc = ln->insertAndSplit(key, rid, *sib, sib_key);

        //check if insert&split is successful
		if (rc < 0)
			return rc;

            //set sibling pid to endPid
		PageId sib_pid = pf.endPid();

        //write the pid and pagefile
		rc = sib->write(sib_pid, pf);

        //check if write is successful
		if (rc < 0)
			return rc;

        //is this step necessary?
        //doesn't insert&split take care of setting the next pointer?
		ln->setNextNodePtr(sib_pid);

        //write to disk
		rc = ln->write(pid, pf);

        //check for write success
		if (rc < 0)
			return rc;

		return 0;

	}

	return 0;

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

        //create a new instance of leafnode
		BTLeafNode *ln = new BTLeafNode;

        //insert this key&rid
		rc = ln->insert(key, rid);

        //check if insert is successful
		if (rc < 0)
			return rc;
	
        //set the rootPid to 1
		rootPid = 1;

        //write this new rootPid 
		rc = ln->write(rootPid, pf);

        //check if write is successful
		if (rc < 0)
			return rc;

        //set treeHeight to 1
		treeHeight = 1;
	}
	else {
		int new_key;
		PageId new_pid;
		rc = insertHelper(rid, key, rootPid, new_key, new_pid, 1);

		// If the node had to be split then should initialize a new root
		if (rc == RC_NODE_FULL)
		{
            //new instance of nonleafnode
			BTNonLeafNode *new_root = new BTNonLeafNode;

            //initialize the new root
			rc = new_root->initializeRoot(rootPid, new_key, new_pid);

            //check if initialization is successful
			if (rc < 0)
				return rc;

                //set rootPid to the end
			rootPid = pf.endPid();

            //write to disk
			rc = new_root->write(rootPid, pf);

            //check if write is successful
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

RC BTreeIndex::locateHelper(int searchKey, IndexCursor& cursor, int counter, int treeHeight, PageId pid) {
	RC rc;
	if (counter == treeHeight) { // leaf node
        //new instance of LeafNode
		cursor.pid = pid;
		BTLeafNode *ln = new BTLeafNode();
		if((rc = ln->read(pid, pf)) < 0) return rc;
		rc = ln->locate(searchKey, cursor.eid);
        //check for locate's success
		if (rc < 0)
			return rc;
	}
	else { // nonleaf node
        //new instance of nonleafnode
		BTNonLeafNode *n = new BTNonLeafNode();
		if((rc = n->read(pid,pf)) < 0) return rc;
		
		PageId c_pid;
		rc = n->locateChildPtr(searchKey,c_pid);
        //check if locateChildPtr succeessful
		if (rc < 0)
			return rc;
        //otherwise just keep going
		return locateHelper(searchKey, cursor, counter+1, treeHeight, c_pid);
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
    //call the recursive function that we made
	rc = locateHelper(searchKey, cursor, 0, treeHeight, rootPid); // puts the cursor at the leaf node
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
    //check if valid cursor
    if (cursor.pid < 0 || cursor.pid >= pf.endPid())
    	return RC_INVALID_CURSOR;

    //create a new BTLeafNode
    BTLeafNode * ln = new BTLeafNode();
    RC rc;

        //check if read successful
    if((rc = ln->read(cursor.pid, pf)) != 0)
    	return rc;
    //check if readLEntry successful
    if((rc = ln->readLEntry(cursor.eid, key, rid)) != 0)
    	return rc;

    //check eid against the getKeyCount
    if(cursor.eid == ln->getKeyCount()-1)
    {
        //set the nextNodePtr
    	cursor.pid = ln->getNextNodePtr();
        //set the cursor eid to 0 again
        //otherwise just increment eid
    	cursor.eid = 0;
    }
    else
    	cursor.eid++;

    return 0;

}
