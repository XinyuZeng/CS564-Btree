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
#include "exceptions/page_pinned_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/hash_not_found_exception.h"


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
    scanExecuting = false;
    nextEntry = leafOccupancy;  // set for simplicity in scanNext
    currentPageNum = 0;
    currentPageData = nullptr;

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
        // init headerPage
        Page* headerPage;
        bufMgr->allocPage(file, headerPageNum, headerPage);
        memset(headerPage, 0, headerPage->SIZE);
        metaInfo = (IndexMetaInfo*)headerPage;
        strncpy(metaInfo->relationName, relationName.c_str(), sizeof(metaInfo->relationName));
        metaInfo->attrType = attrType;
        metaInfo->attrByteOffset = attrByteOffset;
        // init rootPage
        Page* rootPage;
        bufMgr->allocPage(file, rootPageNum, rootPage);
        memset(rootPage, 0, rootPage->SIZE);
        metaInfo->rootPageNo = rootPageNum;
        ((LeafNodeInt *)rootPage)->rightSibPageNo = 0;
        // unpin
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        //fill the newly created Blob File using filescan
        FileScan fileScan(relationName, bufMgr);
        RecordId rid;
        try
        {
            int i = 1;
            while(1)
            {
                fileScan.scanNext(rid);
                std::string record = fileScan.getRecord();
                insertEntry(record.c_str() + attrByteOffset, rid);
                ++i;
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
// TODO: think about others need to be destructed
BTreeIndex::~BTreeIndex()
{
    scanExecuting = false;
    bufMgr->flushFile(file);
    if (currentPageNum != 0) {
        try {
            bufMgr->unPinPage(file, currentPageNum, true);
        } catch (PageNotPinnedException e) {

        } catch (HashNotFoundException e) {

        }
    }
    delete file;
    file = nullptr;
}

/**
 * Change the rootPageNum to the newRootPageNum, and change the info in
 * metaPage as well.
 * @param newRootPageNum the new rootPageNum to set in private variable
 */
const void BTreeIndex::changeRootPageNum(const PageId newRootPageNum) {
    rootPageNum = newRootPageNum;
    Page* headerPage;
    bufMgr->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo* metaInfo = (IndexMetaInfo*)headerPage;
    metaInfo->rootPageNo = rootPageNum;
    bufMgr->unPinPage(file, headerPageNum, true);
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
}

/**
 * place the entryPair in the recurrent Leaf node
 * @requires node has space
 * @param entryPair the record-key pair to be placed
 * @param node the leaf node
 */
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
/**
 * place the newChildEntry into the NonLeaf node
 * @requires node has space
 * @param newChildEntry  PageKeyPair to be placed
 * @param node the NonLeafNode
 */
// TODO: figure out whether need to care the left most pointer
const void BTreeIndex::placeNewChild(PageKeyPair<int> &newChildEntry, NonLeafNodeInt *node) {
    int i = 0;
    while (node->pageNoArray[i+1] != 0 && node->keyArray[i] < newChildEntry.key) {
        i++;
    }
    if (node->pageNoArray[i+1] == 0) {  // i is empty, place node here
        node->pageNoArray[i+1] = newChildEntry.pageNo;
        node->keyArray[i] = newChildEntry.key;
    } else {    // i is the first key that greater than entryPair.key
        int j = nodeOccupancy - 1;
        // move all the elements after i rightforward 1
        while (j >= i) {
            if (node->pageNoArray[j+1] != 0) {
                node->pageNoArray[j+2] = node->pageNoArray[j+1];
                node->keyArray[j+1] = node->keyArray[j];
            }
            j--;
        }
        node->pageNoArray[i+1] = newChildEntry.pageNo;
        node->keyArray[i] = newChildEntry.key;
    }
}

/**
 * Recursive function to insert an entry into root.
 * @param isLeaf Current root is leaf or not
 * @param rootPageID PageId of current root
 * @param newChildEntry PageKeyPair used to "pop-up" if split happens
 * @param entryPair Target entry to be inserted
 */
const void BTreeIndex::insertEntryHelper(bool isLeaf, const PageId rootPageID,
                                         PageKeyPair<int> &newChildEntry, RIDKeyPair<int> entryPair) {
    Page* rootPage;
    bufMgr->readPage(file, rootPageID, rootPage);
    if (isLeaf) {
        // insert or split
        LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;
        if (rootNode->ridArray[leafOccupancy - 1].page_number == 0) {  // have space
            placeEntry(entryPair, rootNode);
        } else {    // no space, split
            splitLeaf(rootNode, newChildEntry, entryPair, rootPageID);
        }
    } else {
        // continue searching
        NonLeafNodeInt* nonLeafNode = (NonLeafNodeInt*)rootPage;
        int i = 0;
        for (; i < nodeOccupancy && nonLeafNode->pageNoArray[i+1] != 0; ++i) {
            if (entryPair.key < nonLeafNode->keyArray[i]) { // in this subtree
                break;
            }
        }
        insertEntryHelper(nonLeafNode->level, nonLeafNode->pageNoArray[i],
                newChildEntry, entryPair);
        if (newChildEntry.pageNo != 0) {  // subtree got split
            int j = 0;
            for(; j < nodeOccupancy && nonLeafNode->pageNoArray[j+1] != 0; ++j);
            bool nodeFull = j == nodeOccupancy;
            if (!nodeFull) {
//                put *newchildentry on it, set newchildentry to null
                placeNewChild(newChildEntry, nonLeafNode);
                newChildEntry.set(0, 0);
            } else {
                // split
                splitNonLeaf(nonLeafNode, rootPageID, newChildEntry);
                // newChildEntry point to smallest key on second half
            }
        }
    }
    bufMgr->unPinPage(file, rootPageID, true);
}
/**
 * Split NonLeafNode into two NonLeafNode
 * @param leftNonLeafNode The old NonLeafNode to be split
 * @param leftPageId PageId of the old NonLeafNode
 * @param newChildEntry PageKeyPair of the new allocated node to be pushed up
 */
const void
BTreeIndex::splitNonLeaf(NonLeafNodeInt *leftNonLeafNode, PageId leftPageId, PageKeyPair<int> &newChildEntry) {
    PageId rightPagId;
    Page* rightPage;
    bufMgr->allocPage(file, rightPagId, rightPage);
    memset(rightPage, 0, rightPage->SIZE);
    NonLeafNodeInt *rightNonLeafNode = (NonLeafNodeInt *) rightPage;
    int newEntryIndex = 0;
    while (newEntryIndex < nodeOccupancy && leftNonLeafNode->keyArray[newEntryIndex] < newChildEntry.key)
        ++newEntryIndex;
    int half = (nodeOccupancy + 1) / 2;
    int keyArray[nodeOccupancy + 1];
    PageId pidArray[nodeOccupancy + 2];
    // construct leftNode and newChildEntry into one single array, and set second half to 0
    // left.pageNo[0] does not need to change!
    pidArray[0] = leftNonLeafNode->pageNoArray[0];
    for (int i = 0, j = 0; i < nodeOccupancy + 1; ++i, ++j) {
        if (i == newEntryIndex) {
            keyArray[i] = newChildEntry.key;
            pidArray[i+1] = newChildEntry.pageNo;
            --j;
            continue;
        }
        keyArray[i] = leftNonLeafNode->keyArray[j];
        pidArray[i+1] = leftNonLeafNode->pageNoArray[j+1];
        if (i >= half) {
            leftNonLeafNode->keyArray[j] = 0;
            leftNonLeafNode->pageNoArray[j+1] = 0;
        }
    }
    // if newChildEntry should be in left, then the following set it
    for (int i = 0; i < half; ++i) {
        leftNonLeafNode->keyArray[i] = keyArray[i];
        leftNonLeafNode->pageNoArray[i+1] = pidArray[i+1];
    }
    // set newChildEntry
    newChildEntry.set(rightPagId, keyArray[half]);
    rightNonLeafNode->pageNoArray[0] = pidArray[half+1];
    ++half; // TODO: skip the half one(smallest on right), push up!
    // construct newLeafNode
    for (int i = 0; half < nodeOccupancy + 1; ++i, ++half) {
        rightNonLeafNode->keyArray[i] = keyArray[half];
        rightNonLeafNode->pageNoArray[i+1] = pidArray[half+1];
    }
    if (leftNonLeafNode->level == 1)
        rightNonLeafNode->level = 1;
    // if left is root, then create new root pointing to those two nodes
    // and change rootPageNum. Set newChildEntry to 0 because no need to use
    if (leftPageId == this->rootPageNum) {
        PageId newPageID;
        Page *newPage;
        bufMgr->allocPage(file, newPageID, newPage);
        memset(newPage, 0, newPage->SIZE);
        NonLeafNodeInt *realRoot = (NonLeafNodeInt *)newPage;
        realRoot->level = 0;
        realRoot->keyArray[0] = newChildEntry.key;
        realRoot->pageNoArray[0] = leftPageId;
        realRoot->pageNoArray[1] = rightPagId;
        changeRootPageNum(newPageID);
        newChildEntry.set(0, 0);
        bufMgr->unPinPage(file, newPageID, true);
    }
    bufMgr->unPinPage(file, rightPagId, true);
}

/**
 * Split leftLeafNode into two leaf node, the smallest key in right half and
 * the pageNo of right leaf got copied up by newChildEntry
 * @param leftLeafNode Old node to be split
 * @param newChildEntry Got copied up
 * @param entryPair The new inserted data
 * @param leftPageId PageId of left leaf
 */
const void BTreeIndex::splitLeaf(LeafNodeInt *leftLeafNode, PageKeyPair<int> &newChildEntry, RIDKeyPair<int> entryPair,
                                 PageId leftPageId) {
    PageId rightPageID;
    Page* rightPage;
    bufMgr->allocPage(file, rightPageID, rightPage);
    memset(rightPage, 0, rightPage->SIZE);
    LeafNodeInt *rightLeafNode = (LeafNodeInt *)rightPage;
    int newEntryIndex = 0;
    while (newEntryIndex < leafOccupancy && leftLeafNode->keyArray[newEntryIndex] < entryPair.key)
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
        keyArray[i] = leftLeafNode->keyArray[j];
        ridArray[i] = leftLeafNode->ridArray[j];
        if (i >= half) {
            leftLeafNode->keyArray[j] = 0;
            leftLeafNode->ridArray[j].page_number = 0;
            leftLeafNode->ridArray[j].slot_number = 0;
        }
    }
    // if entryPair should be in oldLeaf, then the following set it
    for (int i = 0; i < half; ++i) {
        leftLeafNode->keyArray[i] = keyArray[i];
        leftLeafNode->ridArray[i] = ridArray[i];
    }
    // construct newLeafNode
    for (int i = 0; half < leafOccupancy + 1; ++i, ++half) {
        rightLeafNode->keyArray[i] = keyArray[half];
        rightLeafNode->ridArray[i] = ridArray[half];
    }
    rightLeafNode->rightSibPageNo = leftLeafNode->rightSibPageNo;
    // set newChildEntry and sibling pointer
    newChildEntry.set(rightPageID, rightLeafNode->keyArray[0]);
    leftLeafNode->rightSibPageNo = rightPageID;

    // if oldLeafNode is root, then create new root pointing to those two nodes
    // and change rootPageNum. Set newChildEntry to 0 because no need to use
    if (leftPageId == this->rootPageNum) {
        PageId newPageID;
        Page *newPage;
        bufMgr->allocPage(file, newPageID, newPage);
        memset(newPage, 0, newPage->SIZE);
        NonLeafNodeInt *realRoot = (NonLeafNodeInt *)newPage;
        realRoot->level = 1;
        realRoot->keyArray[0] = newChildEntry.key;
        realRoot->pageNoArray[0] = leftPageId;
        realRoot->pageNoArray[1] = rightPageID;
        changeRootPageNum(newPageID);
        newChildEntry.set(0, 0);
        bufMgr->unPinPage(file, newPageID, true);
    }
    bufMgr->unPinPage(file, rightPageID, true);
}

/**
 * Find the smallest key value in the subtree
 * @param root Root of the tree
 * @return Smallest key inside root
 */
const int BTreeIndex::findSmallestKey(NonLeafNodeInt *root) {
    PageId targetPageId = root->pageNoArray[0];
    Page *targetPage;
    bufMgr->readPage(file, targetPageId, targetPage);
    PageId result;
    if (root->level == 1) {
        LeafNodeInt *target = (LeafNodeInt *)targetPage;
        result = target->keyArray[0];
    } else {
        NonLeafNodeInt *target = (NonLeafNodeInt *)targetPage;
        result = findSmallestKey(target);
    }
    bufMgr->unPinPage(file, targetPageId, false);
    return result;
}

/**
 * Find the first leaf value in the subtree satisfying scan criteria
 * @param root Root of the tree
 * @return PageId of first leaf
 */
const PageId BTreeIndex::findFirstLeaf(NonLeafNodeInt *root) {
    int targetKey = lowOp == GT ? lowValInt + 1 : lowValInt;
    PageId targetPageId = 0;
    int i = 0;
    for (i = 0; i < nodeOccupancy && root->pageNoArray[i+1] != 0; ++i) {
        if (root->keyArray[i] > targetKey) {
            targetPageId = root->pageNoArray[i];
            break;
        }
    }
    if (targetPageId == 0)
        targetPageId = root->pageNoArray[i];
    if (root->level == 1) {
        return targetPageId;
    } else {
        Page *targetPage;
        PageId result;
        bufMgr->readPage(file, targetPageId, targetPage);
        NonLeafNodeInt *target = (NonLeafNodeInt *)targetPage;
        result = findFirstLeaf(target);
        bufMgr->unPinPage(file, targetPageId, false);
        return result;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    if (*(int *)lowValParm > *(int *)highValParm) {
        throw BadScanrangeException();
    }
    if (!(lowOpParm == GT || lowOpParm == GTE) ||
        !(highOpParm == LT || highOpParm == LTE)) {
        throw BadOpcodesException();
    }
    lowValInt = *(int *)lowValParm;
    highValInt = *(int *)highValParm;
    lowOp = lowOpParm;
    highOp = highOpParm;
    scanExecuting = true;
    if (rootPageNum < 2)
        return;
    if (rootPageNum == 2) {  // TODO: doable?
        currentPageNum = 2;
    } else {
        Page *rootPage;
        bufMgr->readPage(file, rootPageNum, rootPage);
        currentPageNum = findFirstLeaf((NonLeafNodeInt *)rootPage);
        bufMgr->unPinPage(file, rootPageNum, false);
    }
    bufMgr->readPage(file, currentPageNum, currentPageData);
    while (1) {
        // read through entries in current page
        // if find first entry, get it
        int i = 0;
        bool getFirst = false;
        bool alreadyExceed = false;
        LeafNodeInt *leafNodeInt = (LeafNodeInt *) currentPageData;
        for (; i < leafOccupancy && leafNodeInt->ridArray[i].page_number != 0; ++i) {
            if (highOpParm == LT) {
                if (leafNodeInt->keyArray[i] >= highValInt) {
                    alreadyExceed = true;
                    break;
                }
            } else {
                if (leafNodeInt->keyArray[i] > highValInt) {
                    alreadyExceed = true;
                    break;
                }
            }
            if (lowOpParm == GT) {
                if (leafNodeInt->keyArray[i] > lowValInt) {
                    nextEntry = i;
                    getFirst = true;
                    break;
                }
            } else {
                if (leafNodeInt->keyArray[i] >= lowValInt) {
                    nextEntry = i;
                    getFirst = true;
                    break;
                }
            }
        }
        if (alreadyExceed) {
            bufMgr->unPinPage(file, currentPageNum, false);
            throw NoSuchKeyFoundException();
        }
        if (getFirst) {
            break;
        } else {    // not get the entry
            PageId nextPageNum = leafNodeInt->rightSibPageNo;
            bufMgr->unPinPage(file, currentPageNum, false);
            if (nextPageNum == 0) {
                // no next page, not found such key
                throw NoSuchKeyFoundException();
            }
            currentPageNum = nextPageNum;
            bufMgr->readPage(file, currentPageNum, currentPageData);
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    if (scanExecuting == false) {
        throw ScanNotInitializedException();
    }
    // read currentPageData, getNextEntry if valid
    // continue to next page if valid else break
    if (nextEntry < 0 || nextEntry >= leafOccupancy) {
        throw IndexScanCompletedException();
    }
    LeafNodeInt *leafNodeInt = (LeafNodeInt *)currentPageData;
    outRid = leafNodeInt->ridArray[nextEntry];
    // still within one leaf and it has data
    if (nextEntry + 1 < leafOccupancy &&
        leafNodeInt->ridArray[nextEntry + 1].page_number != 0) {
        if (highOp == LT) {
            if (leafNodeInt->keyArray[nextEntry + 1] < highValInt) {
                nextEntry++;
            } else {
                nextEntry = -1;
            }
        } else {
            if (leafNodeInt->keyArray[nextEntry + 1] <= highValInt) {
                nextEntry++;
            } else {
                nextEntry = -1;
            }
        }
    } else {    // go to next page or report finish
        if (leafNodeInt->rightSibPageNo == 0) {
            nextEntry = -1;
        } else {
            PageId nextPageNum = leafNodeInt->rightSibPageNo;
            bufMgr->unPinPage(file, currentPageNum, false);
            currentPageNum = nextPageNum;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            LeafNodeInt *leafNodeInt = (LeafNodeInt *)currentPageData;
            nextEntry = 0;
            // need to check first entry in next leaf is valid or not
            if (highOp == LT) {
                if (leafNodeInt->keyArray[nextEntry] >= highValInt) {
                    nextEntry = -1;
                }
            } else {
                if (leafNodeInt->keyArray[nextEntry] > highValInt) {
                    nextEntry = -1;
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    if (scanExecuting == false) {
        throw ScanNotInitializedException();
    }
    try {
        scanExecuting = false;
        bufMgr->unPinPage(file, currentPageNum, false);
    } catch (PageNotPinnedException e) {

    } catch (HashNotFoundException e) {

    }

}

}
