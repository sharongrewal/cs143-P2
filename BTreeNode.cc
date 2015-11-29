#include "BTreeNode.h"

using namespace std;

BTLeafNode::BTLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	RC rc = pf.read(pid, buffer);
	return rc; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	RC rc = pf.write(pid, buffer);
	return rc;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	/*
	 * Note: Leaf nodes just need to read the keys for each entry
	 * in the node (until empty or until page_id pointer)
	*/
	leafNodeEntry * l = (leafNodeEntry*) buffer;
	for(int i = 0; i < MAX_KEYS; i++)
	{
		if(l->key == 0)
			return i;
	        l++;
	}
	return MAX_KEYS;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	int count = getKeyCount();
	// ISSUE: If the node is full, do we need to split or just report error?
	if (count >= MAX_KEYS)
    	return RC_NODE_FULL;

	int eid;
	RC rc = locate(key, eid);
	if (rc < 0) {
		if (rc != RC_NO_SUCH_RECORD)
    		return rc;
    	else eid = count;
    }

    int* bufferPtr = (int*) buffer;

    // Moves keys over by one index, and inserts new values into correct position.
	for (int key_index = count * ENTRY_SIZE - 1; key_index >= eid * ENTRY_SIZE; key_index--) {
    	buffer[key_index + ENTRY_SIZE] = buffer[key_index];
    	if (key_index == eid * ENTRY_SIZE) {
    		*(bufferPtr + key_index) = key;
			*(bufferPtr + key_index + sizeof(int)) = rid.pid;
			*(bufferPtr + key_index + sizeof(int) + sizeof(int)) = rid.sid;
    	}
	}

	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{

	RC rc;
	int numKeys = getKeyCount();

	// Check to make sure the node is full. Otherwise, error.
	// Check to make sure sibling node is empty. Otherwise, error.
	// ISSUE: Can we make the assumption that we would only want to split if the node was full?
	if (numKeys < MAX_KEYS)
		return RC_INVALID_FILE_FORMAT;
    else if (sibling.getKeyCount() != 0)
		return RC_INVALID_ATTRIBUTE;
	
	// Get the index of the middle of the full node, to prepare to split node in half with sibling.
	int sib_key;
	RecordId sib_rid;
	int mid = (numKeys + 1) / 2;
	int* bufferPtr = (int*) buffer;

	// Move second half of current node into sibling node; delete moved data from original node.
	for (int key_index = mid * ENTRY_SIZE; key_index < numKeys * ENTRY_SIZE; key_index++) {
		// read from original node, insert into sibling node
		rc = readLEntry(key_index/ENTRY_SIZE, sib_key, sib_rid);
		if (rc < 0) return rc;
		rc = sibling.insert(sib_key, sib_rid);
		if (rc < 0) return rc;

		// delete from original node
		*(bufferPtr + key_index) = -1;
		*(bufferPtr + key_index + sizeof(int)) = -1;
		*(bufferPtr + key_index + sizeof(int) + sizeof(int)) = -1;
	}

	// Set sibling's pointer to next sibling node.
	sibling.setNextNodePtr(getNextNodePtr());
	// Set current node's pointer to new sibling node.
	// setNextNodePtr(endPid()-1);

	// Insert new (key, rid) pair into correct position
	// ISSUE: Do we need to push up values to parent node?
	if (key < siblingKey) {
		insert(key, rid);
	} else sibling.insert(key, rid);

	return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	leafNodeEntry *l = (leafNodeEntry*) buffer;
	int c_key = getKeyCount();
	for(int c = 0; c < c_key; c++)
	{
	 	if(l->key == searchKey)
		{
			eid = c;
			return 0;
		}
	 	if(l->key > searchKey)
		{
			eid = c;
			return RC_NO_SUCH_RECORD;	
		}
		l++;
	}
	eid = c_key;
	return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readLEntry(int eid, int& key, RecordId& rid)
{ 
	// NOTE: eid is the index of the leaf node

	// TODO: Not really sure if we should be using buffer or another
	// structure?? HERE, buffer represents the leaf node.

	// TODO: Make a struct for (key, rid)?
	// Right now, it assumes that we assume key, rid, sid are adjacent
	// values in an array.

	// Is it necessary to check if eid exceeds number of keys in page?

	if (eid < 0 || eid >= MAX_KEYS)
		return RC_INVALID_CURSOR;

	int* bufferPtr = (int*) buffer;
	int index = eid * ENTRY_SIZE;

	key = *(bufferPtr + index);
	rid.pid = *(bufferPtr + index + sizeof(PageId));
	if (rid.pid < 0)
		return RC_INVALID_PID;
	rid.sid = *(bufferPtr + index + sizeof(PageId) + sizeof(int));
	if (rid.sid < 0)
		return RC_INVALID_RID;

	return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	int* p = (int*) buffer;
	p = p + (MAX_KEYS * ENTRY_SIZE);
	return *p;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	// TODO: Check if pid is null? Not really sure what errors arise.
	if (pid < 0)
		return RC_INVALID_PID;

	// Set pointer to end of buffer (node). Set node pointer to pid.
	int* p = (int*) buffer;
	p = p + (MAX_KEYS * ENTRY_SIZE);
	*p = pid;

	return 0;
}

BTNonLeafNode::BTNonLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	RC rc = pf.read(pid, buffer);
	return rc; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	RC rc = pf.write(pid, buffer);
	return rc;
}

/*
 * Read the key from the node based on index.
 * @param index[IN] the index to read the key from
 * @param key[OUT] the key from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::readNLEntry(int index, int& key)
{
	// Is it necessary to check if eid exceeds number of keys in page?
	if (index < 0 || index >= MAX_KEYS)
		return RC_INVALID_CURSOR;

	// Sets ptr to index of key (indicated by index). Gets key value.
	int* bufferPtr = (int*) buffer;
	int total_index = index * ENTRY_SIZE + sizeof(PageId);
	key = *(bufferPtr + total_index);

	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	/*Non-leaf nodes read every other key in the array for a key.*/
	int numKeys = 0;
	int key;
	RC rc;

	// Counts number of keys in node.
	for (int pid = 0; pid < MAX_KEYS; pid++) {
		rc = readNLEntry(pid, key);
		if (key < 0) // TODO: THIS ASSUMES THAT KEYS CAN'T BE NEGATIVE
			break; // NEED TO FIGURE OUT HOW TO MARK KEYS AS NON-EXISTENT
		numKeys++;
	}

	return numKeys;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	int currentCount = getKeyCount();
	if (currentCount >= MAX_KEYS)
    	return RC_NODE_FULL;

    // CHECK THIS!! probs needs more error checking
    int eid = 0;
    non_leafNodeEntry * nl = (non_leafNodeEntry*)(buffer + sizeof(PageId));
    for(int n = 0; n < currentCount; n++)
    {
    	if(nl->key > key)
    		break;
    	nl++;
    	eid++;
    }

    memmove(nl+1, nl, (currentCount - eid) * sizeof(non_leafNodeEntry));

    nl->key = key;
    nl->pid = pid;

	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 

	int currentCount = getKeyCount();
	int midkey_eid = (currentCount - 1)/2;
	int insert_key = 1;


	non_leafNodeEntry* first = (non_leafNodeEntry*) (buffer+sizeof(PageId));
	non_leafNodeEntry* nl = first + midkey_eid;
	non_leafNodeEntry* last = first + currentCount;

    if(key> nl[1].key)
    {
    	midkey_eid++;
    	nl++;
    	insert_key = 0;
    }

    midKey = nl->key;
    sibling.initializeRoot(nl->pid, nl[1].key, nl[1].pid);
    for(int c = midkey_eid+2; c < currentCount; c++)
    {
    	sibling.insert(first[c].key, first[c].pid);
    }

    memset(nl, 0, (currentCount - midkey_eid-1)* sizeof(non_leafNodeEntry));

	if(insert_key == 1)
		insert(key, pid);
	else
		sibling.insert(key,pid);

	return 0; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	//check for errors
    PageId* page = (PageId*) buffer;
   non_leafNodeEntry * nl = (non_leafNodeEntry*) (buffer + sizeof(PageId));
    int key_count = getKeyCount();
    for(int c = 0; c < key_count; c++)
    {
    	if(nl[c].key > searchKey){
    		pid = (c==0) ? *page : nl[c-1].pid;
    		return 0;
    	}
    }
    pid = nl[key_count-1].pid;
    return 0;



 }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	//do we need to make sure buffer is empty?
	memset(buffer, -1, PageFile::PAGE_SIZE);

	// TODO: Do we need to allocate memory for this?
	int* ptr = (int*) buffer;

	non_leafNodeEntry* nl = (non_leafNodeEntry*) (buffer+sizeof(PageId));

	*ptr = pid1;
	nl->pid = pid2;
	nl->key = key;

	/*(ptr + 1) = pid1;
	*(ptr + 2) = key;
	*(ptr + 3) = pid2; */

	return 0;
}
