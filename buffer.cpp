/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
  
  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
    
}

void BufMgr::advanceClock()
{
    
}

void BufMgr::allocBuf(FrameId & frame) 
{
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId temp;
    // look up file and pageid in the hashtable
    try {
        hashTable.lookup(file, pageNo, &temp);
    } catch(HashNotFoundException e1) {
        // if it is not in the buffer pool
        allocBuf(clokhand);
        // read page from file and insert it into buffer pool
        bufPool[clockHand] = (*file).readPage(pageNo);
        // insert it into hashtable
        hashTable.insert(file, pageNo, clockHand);
        // invoke set()
        bufDescTable[clockHand].Set(file, pageNo);
        page = &（bufPool[clockHand]）;
        return;
    }
    // if it is in the buffer pool
    bufDescTable[temp].refbit = true;
    bufDescTable[temp].pinCnt ++;
    page = &(bufPool[temp]);
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    // check whether this page is in the hashtable
    try {
        hashTable.lookup(file, pageNo, &temp);
    } catch(HashNotFoundException e1) {
        // if this page is not in the hash table
        return;
    }
    // if this page is in the hash table
    bufDesc b = bufDescTable[temp];
    if (b.pinCnt == 0)
        throw PageNotPinnedException;
    else {
        // if this page's pin is bigger than zero
        b.pinCnt = b.pinCnt - 1;
        if(dirty == true)
            b.dirty = true;
    }
}

void BufMgr::flushFile(const File* file) 
{
    // go through the frame array
    for(int i = 0; i < numBufs; i++) {
        BufDesc temp = bufDescTable[i];
        // check whether this frame's page belong to the given file
        if(temp.file == file) {
            if(temp.dirty == true) {
                // flush the page into the disk if it is dirty
                // and 
                (*file).writePage(bufPool[i]);
                temp.dirty = false;
            }
            // remove page from the hashtable
            hashTable.remove(file,tmep.pageNo);
            // clear the bufdesc
            temp.Clear();
        }
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    
    
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
