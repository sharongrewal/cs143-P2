/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}


/*

The select() function is called when the user issues the SELECT command.

The attribute in the SELECT clause is passed as the first input parameter attr
(attr=1 means "key" attribute, attr=2 means "value" attribute, attr=3 means "*",
and attr=4 means "COUNT(*)").

The table name in the FROM clause is passed as the second input parameter table.

The conditions listed in the WHERE clause are passed as the input parameter conds

*/

RC SqlEngine::selectHelper(BTreeIndex* btree, int attr, const string& table, const vector<SelCond>& cond)
{

RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  IndexCursor cursor;

 

  RC        rc;
  int       key;     
  string    value;
  bool index = false;
  int select_count = 0;



  // variables for conditions involving keys
  int       keyComp  = -1;
  int       low_k    = -1;
  int       high_k   = -1;
  vector<int> eq_cond_k;
  bool has_eq_k = false; // checks for  multiple equality statements

  // variables for conditions involving values
  string    valComp  = "";
  string    low_v    = "";
  string    high_v   = "";
  vector<string> eq_cond_v;
  bool has_eq_v = false; // checks for  multiple equlity statements

  /*if((rc = btree->locate(low_k, cursor)) != 0 && rc != -1012) { fprintf(stdout, "rc: %d // locating is horrible with low key %d\n", rc, low_k);return rc;}

    for(int k = 0; k < 7; k++)
    {
      if((rc = btree->readForward(cursor, key, rid)) == 0)
        fprintf(stdout,"Key: %d, RID.p: %d, RID.s: %d/// ", key, rid.pid, rid.sid);
    else { fprintf(stdout,"Key: %d, cursor.pid: %d, RC: %d\n", key, cursor.pid, rc); return rc; }
  }*/

  
  for(int c = 0; c < cond.size(); c++)
  {
    
    if(cond[c].attr == 1){
      keyComp = atoi(cond[c].value);
      switch(cond[c].comp)
      {
        case SelCond::EQ:
          low_k = high_k = keyComp;
          break;
        case SelCond::NE:
         // eq_cond_k.push_back((cond[c]));
          break;
        case SelCond::LT:
          if(keyComp - 1 < high_k || high_k != -1)
            high_k = keyComp - 1;
          break;
        case SelCond::GT:
          if(keyComp + 1 > low_k)
              low_k = keyComp + 1;
          break;
        case SelCond::GE:
          if(keyComp > low_k)
              low_k = keyComp;
          break;
        case SelCond::LE:
          if(keyComp < high_k || high_k != -1)
            high_k = keyComp;
          break;
      }
    }
    
  }

      // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

 
   /* fprintf(stdout, "low_k is %d\n", low_k);
    fprintf(stdout, "attr is %d\n", attr);
    fprintf(stdout, "high_k is %d\n", high_k);*/
    if((rc = btree->locate(low_k, cursor)) != 0 && rc != RC_NO_SUCH_RECORD) { fprintf(stdout, "locating is horrible\n");return rc;}
    /*fprintf(stdout, "located low_k %d successfully\n", low_k);
    fprintf(stdout, "locate: cursor.eid is %d cursor.pid is %d\n", cursor.eid, cursor.pid);*/

    for(;;)
    {
      if((rc = btree->readForward(cursor, key, rid)) != 0) {fprintf(stderr,"readingForward is horrible because %d\n", rc); return rc;}
      /*fprintf(stdout, "readForward: rid.pid is %d, ", rid.pid);
      fprintf(stdout, "rid.sid is %d, ", rid.sid);
      fprintf(stdout, "key is %d\n", key);
      fprintf(stdout, "readFoward: cursor.eid is %d cursor.pid is %d\n", cursor.eid, cursor.pid); */
      

      if(key > high_k && high_k != -1)
        break;

      //rid.pid = cursor.pid++;
      //rid.sid = 0;
      
     
      if(attr == 2 || attr == 3) {
        if((rc = rf.read(rid, key, value)) != 0)
          {
            fprintf(stderr, "reading failed, rc: %d\n", rc);
            return rc;
          }
        }

      //fprintf(stdout, "key is %d value is %s\n", key, value.c_str());
      switch(attr)
      {
        case 1:
          fprintf(stdout, "%d\n", key);
          break;
        case 2:
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
        default:
          break;
      }
      select_count++;

    }
  

  if(attr == 4)
    fprintf(stdout, "%d\n", select_count);
  rc = 0;

  return rc;

 
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  BTreeIndex *btree = new BTreeIndex(table + ".idx", 'r');
  rc = selectHelper(btree, attr, table, cond);
  if(rc != 0) {
    fprintf(stdout, "selectHelper failed\n");
    return rc;
  }
  //else 
   // return rc;

  
/*
  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
      case 1:
        diff = key - atoi(cond[i].value);
        break;
      case 2:
        diff = strcmp(value.c_str(), cond[i].value);
        break;
      }

      // skip the tuple if any condition is not met
      switch (cond[i].comp) {
      case SelCond::EQ:
        if (diff != 0) goto next_tuple;
        break;
      case SelCond::NE:
        if (diff == 0) goto next_tuple;
        break;
      case SelCond::GT:
        if (diff <= 0) goto next_tuple;
        break;
      case SelCond::LT:
        if (diff >= 0) goto next_tuple;
        break;
      case SelCond::GE:
        if (diff < 0) goto next_tuple;
        break;
      case SelCond::LE:
        if (diff > 0) goto next_tuple;
        break;
      }
    }

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;
  */

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* our implementation */

    //with index option not specified; only worry about loadfile and table
    //reading loadfile use fstream or fgets
  //for now assume that index will always be false


  //variables for files
  ifstream curr_file;
  RecordFile rec_file;

 //table values
   int key;
   string value;
   RecordId rid;


 //RecordFile status variables
 RC rc=0;
 RC r_close;


 string line_buffer; //buffer for reading from loadfile

 int line_num = 0; //keep track which line we're on

  //attempt to open the file
  curr_file.open(loadfile.c_str(), std::ifstream::in);

  if(!curr_file.is_open()) //if opening the file failed
  {	
	return RC_FILE_OPEN_FAILED;

  } 
 if((rc = rec_file.open((table + ".tbl").c_str(), 'w')) != 0)
 {	fprintf(stderr, "Error opening record file for table %s\n", table.c_str());
        return rc;
 }

  BTreeIndex * tree_index;
  if (index == true) {
   tree_index = new BTreeIndex(table + ".idx", 'w');
    if (rc < 0) {
      tree_index->close();
      return rc;
    }
  }
 
 while(!curr_file.eof()) //while not end of file
{
  getline(curr_file, line_buffer);

  if(line_buffer == "")
     break; //move to next non empty string

  //start to parse the file otherwise
  if((rc = parseLoadLine(line_buffer, key, value)) != 0) {
    fprintf(stderr, "Error parsing from loadfile %s at line %i\n", 
          loadfile.c_str(), line_num);
  }
  else {
  //append the line 
    if((rc = rec_file.append(key, value, rid)) != 0) {
      fprintf(stderr, "Error appending to table %s\n", table.c_str());
      break;
    }
    if (index == true) {
      rc = tree_index->insert(key, rid);
      if (rc < 0) {
        fprintf(stderr, "Error inserting data into index for table %s\n", table.c_str());
        rec_file.close();
	tree_index->close();
	curr_file.close();
        return rc;
      }
    }
  }
	line_num++; //increment line_num
}

//attempt to close the file now
curr_file.close();

//close the RecordFile as well
if((r_close = rec_file.close()) != 0)
{
	return r_close;
}



  return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
