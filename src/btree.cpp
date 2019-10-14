/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    std::ostringstream idxStr;
    idxStr << relationName << "." << attrByteOffset;
    std::string indexName = idxStr.str();   // indexName is the name of the index file
    outIndexName = idxStr.str();
    this->bufMgr = bufMgrIn;
    this->attrByteOffset = attrByteOffset;
    attributeType = attrType;
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    scanExecuting = false;  // TODO: figure why

    // create blobfile, fill in metainfo, etc
    bool fileExist = true;
    try {
        this->file = new BlobFile(indexName, false);
    } catch (FileNotFoundException e) {
        this->file = new BlobFile(indexName, true);
        fileExist = false;
    }

    IndexMetaInfo* metaInfo;

    if (fileExist) {
        headerPageNum = file->getFirstPageNo();
        Page* headerPage;
        bufMgr->readPage(file, headerPageNum, headerPage);
        metaInfo = (IndexMetaInfo*)headerPage;
        if (metaInfo->attrByteOffset != attrByteOffset ||
        metaInfo->attrType != attrType ||
        strcmp(metaInfo->relationName, relationName.c_str()) != 0) {
            throw BadIndexInfoException("metaInfo does not match");
        }
        rootPageNum = metaInfo->rootPageNo;
        bufMgr->unPinPage(file, headerPageNum, false);
    } else {
//        Not necessary
//        headerPageNum = 1;
//        rootPageNum = 2;
        // init headerPage
        Page* headerPage;
        bufMgr->allocPage(file, headerPageNum, headerPage);
        metaInfo = (IndexMetaInfo*)headerPage;
        strncpy(metaInfo->relationName, relationName.c_str(), sizeof(metaInfo->relationName));
        metaInfo->attrType = attrType;
        metaInfo->attrByteOffset = attrByteOffset;
        // init rootPage
        Page* rootPage;
        bufMgr->allocPage(file, rootPageNum, rootPage);
        metaInfo->rootPageNo = rootPageNum;
        // unpin
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        //fill the newly created Blob File using filescan
        FileScan fileScan(relationName, bufMgr);
        RecordId rid;
        try
        {
            while(1)
            {
                fileScan.scanNext(rid);
                std::string record = fileScan.getRecord();
//                insertEntry(record.c_str() + attrByteOffset, rid);
            }
        }
        catch(EndOfFileException e)
        {
            // save Btee index file to disk
            bufMgr->flushFile(file);
        }
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    PageKeyPair<int> newEntry {0, 0};
    RIDKeyPair<int> entryPair;
    entryPair.set(rid, *(int *)key);
    insertEntryHelper(rootPageNum == 2, rootPageNum, newEntry, entryPair);
    rootPageNum = newEntry.pageNo != 0 ? newEntry.pageNo : rootPageNum;
    Page* headerPage;
    bufMgr->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo* metaInfo = (IndexMetaInfo*)headerPage;
    metaInfo->rootPageNo = rootPageNum;
    bufMgr->unPinPage(file, headerPageNum, true);
}

const void BTreeIndex::placeEntry(RIDKeyPair<int> entryPair, LeafNodeInt *node) {
    int i = 0;
    while (node->ridArray[i].page_number != 0 && node->keyArray[i] < entryPair.key) {
        i++;
    }
    if (node->ridArray[i].page_number == 0) {  // i is empty, place node here
        node->ridArray[i] = entryPair.rid;
        node->keyArray[i] = entryPair.key;
    } else {    // i is the first key that greater than entryPair.key
        int j = leafOccupancy - 1;
        // move all the elements after i rightforward 1
        while (j >= i) {
            if (node->ridArray[j].page_number != 0) {
                node->ridArray[j+1] = node->ridArray[j];
                node->keyArray[j+1] = node->keyArray[j];
            }
            j--;
        }
        node->ridArray[i] = entryPair.rid;
        node->keyArray[i] = entryPair.key;
    }
}

const void BTreeIndex::insertEntryHelper(bool isLeaf, const PageId rootPageID,
        PageKeyPair<int> newChildEntry, RIDKeyPair<int> entryPair) {
    Page* rootPage;
    bufMgr->readPage(file, rootPageID, rootPage);
    if (isLeaf) {
        // insert or split
        LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;
        if (rootNode->ridArray[leafOccupancy - 1].page_number == 0) {  // have space
            placeEntry(entryPair, rootNode);
        } else {    // no space, split
            PageId newPageID;
            Page* newPage;
            bufMgr->allocPage(file, newPageID, newPage);
            splitLeaf(rootNode, (LeafNodeInt *) newPage, newChildEntry, entryPair);
            bufMgr->unPinPage(file, newPageID, true);
        }
    } else {
        // continue searching
        NonLeafNodeInt* nonLeafNode = (NonLeafNodeInt*)rootPage;
        int i = 0;
        for (; i < nodeOccupancy && nonLeafNode->pageNoArray[i] != 0; ++i) {
            if (entryPair.key < nonLeafNode->keyArray[i]) { // in this subtree
                break;
            }
        }
        if (i == nodeOccupancy || nonLeafNode->pageNoArray[i] == 0) {   // last subtree
            insertEntryHelper(nonLeafNode->level, nonLeafNode->pageNoArray[i+1],
                    newChildEntry, entryPair);
        } else {
            insertEntryHelper(nonLeafNode->level, nonLeafNode->pageNoArray[i],
                              newChildEntry, entryPair);
        }
        if (newChildEntry.pageNo != 0) {  // subtree got split
//            if ()
        }
    }
    bufMgr->unPinPage(file, rootPageID, true);
}

const void BTreeIndex::splitLeaf(LeafNodeInt *oldLeafNode,
        LeafNodeInt *newLeafNode, PageKeyPair<int> newChildEntry,
                                 RIDKeyPair<int> entryPair) {
    int newEntryIndex = 0;
    while (newEntryIndex < leafOccupancy && oldLeafNode->keyArray[newEntryIndex] < entryPair.key)
        ++newEntryIndex;
    int half = (leafOccupancy + 1) / 2;
    int keyArray[leafOccupancy + 1];
    RecordId ridArray[leafOccupancy + 1];
    // construct oldLeaf and entryPair into one single array, and set second half to 0
    for (int i = 0, j = 0; i < leafOccupancy + 1; ++i, ++j) {
        if (i == newEntryIndex) {
            keyArray[i] = entryPair.key;
            ridArray[i] = entryPair.rid;
            --j;
            continue;
        }
        keyArray[i] = oldLeafNode->keyArray[j];
        ridArray[i] = oldLeafNode->ridArray[j];
        if (i >= half) {
            oldLeafNode->keyArray[j] = 0;
            oldLeafNode->ridArray[j].page_number = 0;
            oldLeafNode->ridArray[j].slot_number = 0;
        }
    }
    // if entryPair should be in oldLeaf, then the following set it
    for (int i = 0; i < half; ++i) {
        oldLeafNode->keyArray[i] = keyArray[i];
        oldLeafNode->ridArray[i] = ridArray[i];
    }
    // construct newLeafNode
    for (int i = 0; half < leafOccupancy + 1; ++i, ++half) {
        newLeafNode->keyArray[i] = keyArray[half];
        newLeafNode->ridArray[i] = ridArray[half];
    }
    // set newChildEntry and sibling pointer
    newChildEntry.set(newLeafNode->ridArray[0].page_number, newLeafNode->keyArray[0]);
    oldLeafNode->rightSibPageNo = ((Page *)newLeafNode)->page_number();
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
