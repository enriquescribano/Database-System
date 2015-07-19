#include "record_scan.h"

#include <stdlib.h>
#include <stdio.h>

//This method is used to carry out the scanning and to close the scan
//returning the node we have to check or close.
AUX_Scan *search(RM_ScanHandle *sHandle, PTR_Scan sEntry){
    PTR_Scan pNode = NULL;
    PTR_Scan cNode = sEntry;
    while(cNode!=NULL && cNode->sHandle!=sHandle){
        pNode=cNode;
        cNode=cNode->nextScan;
    }

    return cNode->auxScan;
}
//This method is used to start the scan
RC insert(RM_ScanHandle *sHandle, PTR_Scan *sEntry, AUX_Scan *auxScan){
    //previous node
    PTR_Scan pNode=NULL;
    //current node
    PTR_Scan cNode=*sEntry;
    //new node in 
    PTR_Scan nNode=(Scan_Entry *)malloc(sizeof(Scan_Entry));

    //setting the values of the new node
    nNode->sHandle=sHandle;
    nNode->auxScan=auxScan;

    while(cNode!=NULL){
        pNode=cNode;
        cNode=cNode->nextScan;
    }

    if(pNode==NULL) *sEntry=nNode;
    else pNode->nextScan=nNode;

    return RC_OK;
}
//This method is used to close the scan
RC delete(RM_ScanHandle *sHandle, PTR_Scan *sEntry){
    //current node to be deleted
    PTR_Scan cNode=*sEntry;

    if(cNode!=NULL){
        PTR_Scan temptr=cNode;
        *sEntry=cNode->nextScan;
    }
    return RC_OK;
}

