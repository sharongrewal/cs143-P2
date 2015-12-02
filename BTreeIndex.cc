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

void BTreeIndex::save_tree()
{

	char temp[PageFile::PAGE_SIZE];
	bzero(temp, PageFile::PAGE_SIZE);
	int* p = (int*) temp;

	*p = rootPid;
	*(p+1) = treeHeight;
	pf.write(0, temp);
}

RC BTreeIndex::insertHelper(const RecordId& rid, int key, PageId pid, int &new_key, PageId &new_pid, int curr_height)
{
	RC rc;
	new_key = -1;
	new_pid = -1;

    //check if leaf node
	if (curr_height == treeHeight) 
	{
		//fprintf(stdout, "insert into leaf node\n");
		BTLeafNode* leaf = new BTLeafNode();
		if((rc = leaf->read(pid, pf)) != 0) return rc;
		//fprintf(stdout, "read successful\n");
	
		if((rc = leaf->insert(key, rid)) == RC_NODE_FULL)
		{	
			//fprintf(stdout, "node is full, must split\n");
			BTLeafNode * sib = new BTLeafNode();
			if((rc = leaf->insertAndSplit(key, rid, *sib, new_key)) != 0) return rc;
			//fprintf(stdout, "insertAndSplit is successful\n");
			new_pid = pf.endPid();
			PageId temp = leaf->getNextNodePtr();
			sib->setNextNodePtr(temp);
			if((rc = leaf->write(pid, pf)) != 0) return rc;
			//fprintf(stdout, "write to leafnode is successful\n");
			if((rc = sib->write(new_pid, pf)) != 0) return rc;
			//fprintf(stdout, "write to sibling leafnode is successful\n");
			if(curr_height == 0)
			{
				BTNonLeafNode * new_root = new BTNonLeafNode();
				if((rc = new_root->initializeRoot(pid, new_key, new_pid)) != 0) return rc;
				PageId nroot_pid = pf.endPid();
				if((rc = new_root->write(nroot_pid, pf)) != 0) return rc;
				rootPid = nroot_pid;
				treeHeight++;
				save_tree();
				delete new_root;
			}
			delete sib;
		}
	
		if((rc = leaf->write(pid, pf)) != 0) return rc;
		//fprintf(stdout, "write to leaf node\n");
     	delete leaf;
	}
	else{
		//fprintf(stdout, "nonleafnode\n");
		BTNonLeafNode* nl = new BTNonLeafNode();
		if((rc = nl->read(pid, pf)) != 0) return rc;
		//fprintf(stdout, "read nonleaf successful\n");
		PageId c_pid;
		if((rc = nl->locateChildPtr(key, c_pid)) != 0) return rc;
		//fprintf(stdout, "locateChildPtr successful\n");
		PageId csib_pid;
		int csib_key;
		if((rc = insertHelper(rid, key, c_pid, csib_key, csib_pid, curr_height+1)) != 0) return rc;
		//fprintf(stdout, "insertHelper succeeded\n");
		if(csib_key > 0)
		{
			if((rc = nl->insert(csib_key, csib_pid)) == RC_NODE_FULL){
				BTNonLeafNode * sib = new BTNonLeafNode();
				if((rc = nl->insertAndSplit(csib_key, csib_pid, *sib, new_key)) != 0) return rc;

				new_pid = pf.endPid();
				if((rc = nl->write(pid, pf)) != 0) return rc;
				

				if(curr_height == 0)
				{
					BTNonLeafNode * n_root = new BTNonLeafNode();
					if((rc = n_root->initializeRoot(pid, new_key, new_pid)) != 0) return rc;

					PageId nroot_pid = pf.endPid();
					if((rc = n_root->write(nroot_pid, pf)) != 0) return rc;
					rootPid = nroot_pid;
					treeHeight++;
					save_tree();
					delete n_root;
				}
				delete sib;
			}
			if((rc = nl->write(pid, pf)) != 0) return rc;
			
		}
		delete nl;
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
	RC rc;

	PageId new_pid;
	int new_key;
	rc = insertHelper(rid, key, rootPid, new_key, new_pid, 0);
	return rc;
       
            

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

   // return 0;
}

RC BTreeIndex::locateHelper(int searchKey, IndexCursor& cursor, int counter, int treeHeight, PageId pid) {
	RC rc;
	if (counter == treeHeight) { // leaf node
        //new instance of LeafNode
        
		cursor.pid = pid;
		BTLeafNode *ln = new BTLeafNode();
		if((rc = ln->read(pid, pf)) < 0) return rc;
		//fprintf(stdout, "locatehelper: read from leafnode is successful\n");
		rc = ln->locate(searchKey, cursor.eid);
        //check for locate's success
		if (rc < 0)
			return rc;
		//fprintf(stdout, "locatehelper: locate from leafnode is successful\n");
	}
	else { // nonleaf node
        //new instance of nonleafnode
        
		BTNonLeafNode *n = new BTNonLeafNode();
		if((rc = n->read(pid,pf)) < 0){
			//fprintf(stdout, "locatehelper: read from nonleafnode returns %d", rc);
			return rc;
		}
		//fprintf(stdout, "locatehelper: read from nonleaf successful\n");
		PageId c_pid;
		rc = n->locateChildPtr(searchKey, c_pid);
        //check if locateChildPtr succeessful
		if (rc < 0)
			return rc;
		//fprintf(stdout, "locatehelper: locateChildPtr successful\n");
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
    
   // fprintf(stdout, "rootPid is %d\n", rootPid);
	rc = locateHelper(searchKey, cursor, 0, treeHeight, rootPid); // puts the cursor at the leaf node
	if (rc < 0)
		return rc;
	//fprintf(stdout, "locatehelper successful\n");

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
    RC rc = 0;

        //check if read successful
    if((rc = ln->read(cursor.pid, pf)) != 0) {
    	fprintf(stderr, "rc: %d; cursor.pid: %d\n", rc, cursor.pid);return rc;}
    //check if readLEntry successful
    if((rc = ln->readLEntry(cursor.eid, key, rid)) != 0) {
    	fprintf(stderr, "rc: %d; cursor.eid: %d\n", rc, cursor.eid);return rc;}

    
    cursor.eid++;
    //check eid against the getKeyCount
    if(cursor.eid == ln->getKeyCount()+1)
    {
    	if (ln->getNextNodePtr() < cursor.pid)
    		rc = RC_END_OF_TREE;
        //set the nextNodePtr
    	cursor.pid = ln->getNextNodePtr();
        //set the cursor eid to 0 again
        //otherwise just increment eid
    	cursor.eid = 0;
    }
    
    delete ln;
    return rc;

}
