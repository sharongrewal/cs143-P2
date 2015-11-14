#include "BTreeNode.h"

using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNode::read(PageId pid, const PageFile& pf)
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
RC BTNode::write(PageId pid, PageFile& pf)
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
	int numKeys = 0;
	int key;
	RecordId rid;

	for (int eid = 0; eid < MAX_KEYS; eid++) {
		readLEntry(eid, key, rid);
		if (key < 0 || rid.pid < 0 || rid.sid < 0)
			break;
		else numKeys++;
	}

	return numKeys;
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

	for (int k = count * ENTRY_SIZE - 1; k >= eid * ENTRY_SIZE; k--) {
    	buffer[k + ENTRY_SIZE] = buffer[k];
    	if (k == eid * ENTRY_SIZE) {
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
		if (rc = readLEntry(key_index/ENTRY_SIZE, sib_key, sib_rid) < 0
			|| rc = sibling.insert(sib_key, sib_rid) < 0) {
			return rc;
		}
		// delete from original node
		*(bufferPtr + key_index) = -1;
		*(bufferPtr + key_index + sizeof(int)) = -1;
		*(bufferPtr + key_index + sizeof(int) + sizeof(int)) = -1;
	}

	// Set sibling's pointer to next sibling node.
	sibling.setNextNodePtr(getNextNodePtr());

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
	int key;
	RecordId rid;

	for(eid = 0; eid < getKeyCount(); eid++) {
		readEntry(eid, key, rid);
		if (key >= searchKey)
			return 0;
	}

	eid = -1;
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
	rid.sid = *(bufferPtr + index + sizeof(PageId) + sizeof(int));

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

	int* p = (int*) buffer;
	p = p + (MAX_KEYS * ENTRY_SIZE);
	*p = pid;

	return 0;
}

RC BTNonLeafNode::readNLEntry(int pid, int& key)
{
	// Is it necessary to check if eid exceeds number of keys in page?
	if (pid < 0 || pid >= MAX_KEYS)
		return RC_INVALID_CURSOR;

	int* bufferPtr = (int*) buffer;
	// gives index of key
	int index = pid * ENTRY_SIZE + sizeof(PageId);

	key = *(bufferPtr + index);

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

	for (int pid = 0; pid < MAX_KEYS; pid++) {
		readNLEntry(pid, key);
		if (key < 0)
			break;
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

    // TODO: everything lol

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
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	// TODO: Do we need to allocate memory for this?
	int* ptr = (int*) buffer;

	*(ptr + 1) = pid1;
	*(ptr + 2) = key;
	*(ptr + 3) = pid2;

	return 0;
}
