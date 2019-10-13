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
                insertEntry(record.c_str() + attrByteOffset, rid);
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
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

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
