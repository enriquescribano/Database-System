#include "storage_mgr.h"
#include "record_scan.h"
#include "record_mgr.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

//main schema
static Schema mainSchema;
//linked list
static PTR_Scan ptr_Scan=NULL;

//this method returns the total 
//size of a record 
int getRecordSize(Schema *schema) {
  //used for the DT_STRING to let us 
  //know which is the string size
  int *typeLength = schema->typeLength,size = 0,i;
  for (i = 0; i < schema->numAttr; i++) {
      //checking datatypes to sum the total size
      switch(schema->dataTypes[i]){
      case DT_INT: size += sizeof(int); break;
      case DT_FLOAT: size += sizeof(float); break;
      case DT_BOOL: size += sizeof(bool); break;
      case DT_STRING: size += typeLength[i]; break;
    }
  }
  return size+schema->numAttr;
}

//the method creates a new schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
                     int *typeLength, int keySize, int *keys) {
  //creating schema 
  Schema *schema = (Schema*) malloc(sizeof(Schema));
  //assigning values
  schema->numAttr = numAttr;
  schema->attrNames = attrNames;
  schema->dataTypes = dataTypes;
  schema->typeLength = typeLength;
  schema->keySize = keySize;
  schema->keyAttrs = keys; 
  
  return schema;
}

//this method cleans the space 
//occupied by the schema
RC freeSchema(Schema *schema) {
    free(schema);
    return (RC_OK);
}

//the method creates a new record
RC createRecord(Record **record, Schema *schema) {
  //creating the record
  Record *rec= (Record*) malloc(sizeof(Record));
  //geting the size of the record
  int size= getRecordSize(schema);
  //allocating memory size
  rec->data= (char*) malloc(size);
  //filing rec-> data with 0
  memset(rec->data, 0, size);
  *record= rec;

  return (RC_OK);
}

//this method cleans the space 
//occupied by the record
RC freeRecord(Record *record) {
  free(record);
  return RC_OK;
}

//the method deals with attributes, it is used to get an attribute
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
  DataType *dataType = schema->dataTypes;
  int *typeLength = schema->typeLength,offset=0,i;
  char *dt = record->data;
  //creating pointer to the value and allocating in memory
  Value *val = (Value*) malloc(sizeof(Value));

  //calculating the value of the offset
  for (i = 0; i < attrNum; i++) {
    switch(dataType[i]){
      case DT_INT: offset += sizeof(int); break;
      case DT_FLOAT: offset += sizeof(float); break;
      case DT_BOOL: offset += sizeof(bool); break;
      case DT_STRING: offset += typeLength[i]; break;
    }
  }
  //updating the offset
  offset+=(attrNum+1);
  //temp is used to convert datatypes
  char *temp=NULL;  
  //depending on datatype we set the value
  switch(dataType[i]){
    case DT_INT:
      //seting value datatype
      val->dt = DT_INT;
      temp=malloc(sizeof(int)+1);
      strncpy(temp, dt + offset, sizeof(int)); 
      temp[sizeof(int)]='\0';
      //temp being converted to int
      val->v.intV = atoi(temp);
      break;
    case DT_FLOAT:
      //seting value datatype
      val->dt = DT_FLOAT;
      temp=malloc(sizeof(float)+1);
      strncpy(temp, dt + offset, sizeof(float)); 
      temp[sizeof(float)]='\0';
      //temp being converted to float 
      val->v.floatV = (float) *temp;
      break;
    case DT_BOOL:
      //seting value datatype
      val->dt = DT_BOOL;
      temp=malloc(sizeof(bool)+1);
      strncpy(temp, dt + offset, sizeof(bool)); 
      temp[sizeof(bool)]='\0';
      //temp being converted to boolean
      val->v.boolV = (bool) *temp;
      break;
    case DT_STRING:
      //seting value datatype
      val->dt = DT_STRING;
      int size = typeLength[i];
      temp=malloc(size+1);
      strncpy(temp, dt + offset, size); 
      temp[size]='\0';
      val->v.stringV=temp;
      break;
  }
  *value= val;
  return RC_OK;
}


//the method deals with attributes, it is used to set an attribute
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
  int numAttr = schema->numAttr,j,temp,i;
  DataType *dataType = schema->dataTypes;
  int *typeLength = schema->typeLength;
  int offset=0;
  //calculating the value of the offset
  for (i = 0; i < attrNum; i++) {
    switch(dataType[i]){
      case DT_INT: offset += sizeof(int); break;
      case DT_FLOAT: offset += sizeof(float); break;
      case DT_BOOL: offset += sizeof(bool); break;
      case DT_STRING: offset += typeLength[i]; break;
    }
  }
  //updating the offset
  offset+=(attrNum+1);
  
  char *recOffset =record->data;
  if(attrNum==0) {
        // '-' for separating tuples
        recOffset[0]='-';
        recOffset++;
    } else {
        // ' ' for separating records
        recOffset+=offset;
        (recOffset-1)[0]=' ';
    }

  switch(value->dt){
    case DT_INT:
      //storing value->v.intV into recOffset pointer
      sprintf(recOffset,"%d",value->v.intV);
      while(strlen(recOffset)<sizeof(int)) strcat(recOffset,"0");
      for (j=strlen(recOffset)-1,i=0; i < j;i++,j--){
          temp=recOffset[i]; recOffset[i]=recOffset[j];
          recOffset[j]=temp;
      }break;
    case DT_FLOAT:
      //storing value->v.floatV into recOffset pointer
      sprintf(recOffset,"%f",value->v.floatV);
      //filling recOffset with 0s
      while(strlen(recOffset)!=sizeof(float)) strcat(recOffset,"0");
        for (j=strlen(recOffset)-1,i=0; i < j;i++,j--){
            temp=recOffset[i]; recOffset[i]=recOffset[j];
            recOffset[j]=temp;
        }break;
    //storing value->v.boolV into recOffset pointer        
    case DT_BOOL: sprintf(recOffset,"%i",value->v.boolV);break;
    //storing value->v.stringV into recOffset pointer        
    case DT_STRING: sprintf(recOffset,"%s",value->v.stringV);break;
  }
  return RC_OK;
}

//the method deals with records, it is used 
//to insert a record in a page
RC insertRecord (RM_TableData *rel, Record *record){
  int slotNum=0;
  char *space=NULL;
  RID id;
  int pLength,totalrecordlength;  
  //creating a page  
  PageNumber pageNum;
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  SM_FileHandle *sh=(SM_FileHandle *)buffer->mgmtData;
  pageNum=1;
  //calculating the size of the record to calculate the 
  //page number of an empty slot
  totalrecordlength=getRecordSize(rel->schema);
  while(pageNum < sh->totalNumPages){
      pinPage(buffer,pHandle,pageNum);
      pLength=strlen(pHandle->data);
      //empty slot found
      if(PAGE_SIZE-pLength > totalrecordlength)
      {
          slotNum=pLength/totalrecordlength;
          unpinPage(buffer,pHandle);
          break;
      }
      unpinPage(buffer,pHandle);
      pageNum++;
  //appending the file because no empty slots
  }if(slotNum==0){
    pinPage(buffer,pHandle,pageNum + 1);
    unpinPage(buffer,pHandle);
  }
  pinPage(buffer,pHandle,pageNum);
  space=pHandle->data+strlen(pHandle->data);
  //copying record->data to the freespace
  strcpy(space,record->data);
  //marking as dirty
  markDirty(buffer,pHandle);
  unpinPage(buffer,pHandle);
  
  id.page=pageNum;
  id.slot=slotNum;
  //setting slot info
  record->id=id;

  return RC_OK;
}

//the method deals with records, it is used to 
//remove depending on RID
RC deleteRecord (RM_TableData *rel, RID id){
  int slotNum=id.slot;
  //creating pointers to manage the data  
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  PageNumber pageNum=id.page; 
  //size to be removed   
  pinPage(buffer,pHandle,pageNum);
  markDirty(buffer,pHandle);
  unpinPage(buffer,pHandle);
  free(pHandle);
  return RC_OK;
}

//the method deals with records, it is used to update 
//the record with new values
RC updateRecord (RM_TableData *rel, Record *record){
  char *space;
  int recLength;
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  RID id=record->id;
  //slot to be updated
  int slotNum=id.slot;
  //page to be updated
  PageNumber pageNum=id.page;
  //space of slot to be updated
  recLength=getRecordSize(rel->schema);
  pinPage(buffer,pHandle,pageNum);
  //address of updating content
  space=pHandle->data+recLength*slotNum;
  //copying the content
  strncpy(space,record->data,recLength);

  return RC_OK;
}

//the method deals with records, it is used to get a record value
//depending on the RID and assign the value to the record
RC getRecord (RM_TableData *rel, RID id, Record *record){
  int slotNum=id.slot,recLength;
  char *space;
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  //getting the id of the page
  PageNumber pageNum=id.page;
  //getting the size of the record of the schema
  recLength=getRecordSize(rel->schema);
  pinPage(buffer,pHandle,pageNum);
  space=pHandle->data+recLength*slotNum;
  //copying the content
  strncpy(record->data,space,recLength);
  unpinPage(buffer,pHandle);

  return RC_OK;
}

//This method starts a new scan
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  SM_FileHandle *fHandle=(SM_FileHandle *)buffer->mgmtData;
  //allocating the auxiliary scan in memory
  AUX_Scan *aux_scan=(AUX_Scan *)malloc(sizeof(AUX_Scan));
  //Seting values 
  scan->mgmtData= cond;
  scan->rel=rel;
  //setting values of the auxiliary scan
  aux_scan->_slotID=1;
  aux_scan->_sPage=1;
  aux_scan->_numPages=fHandle->totalNumPages;
  aux_scan->pHandle=MAKE_PAGE_HANDLE();
  aux_scan->_recLength=getRecordSize(rel->schema);
  //assigning values of the auxiliary scan to the scan
  return insert(scan,&ptr_Scan,aux_scan);
  
}
//This method scans the records until it finds
//a tuple that matches.
RC next (RM_ScanHandle *scan, Record *record){
  RID id;
  Expr *expression=(Expr *)scan->mgmtData,*l,*r,*auxExpr;
  RM_TableData *re=scan->rel;
  Operator *decision,*decision2;
  AUX_Scan *AUX_Scan=search(scan,ptr_Scan);
  //Column value that has to be revised in the tuple
  Value **cValue=(Value **)malloc(sizeof(Value *));
  //Flag used to know if there is a match
  *cValue=NULL;
  decision=expression->expr.op;
  //depending on the operation of the expression...
  switch(decision->type){
    case OP_COMP_SMALLER:
      l=decision->args[0];
      r=decision->args[1];
      //checking all pages
      while(AUX_Scan->_sPage < AUX_Scan->_numPages){
          pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
          AUX_Scan->_recsPage=strlen(AUX_Scan->pHandle->data)/AUX_Scan->_recLength;
          //checking all records in the page
          while(AUX_Scan->_slotID < AUX_Scan->_recsPage){
              id.slot=AUX_Scan->_slotID;
              id.page=AUX_Scan->_sPage;
              //getting the record of this id
              getRecord(re,id,record);
              //getting the aattribute of the record to see if it matches
              getAttr(record,re->schema,r->expr.attrRef,cValue);
              //match found
              if((re->schema->dataTypes[r->expr.attrRef]==DT_INT)&&(l->expr.cons->v.intV>cValue[0]->v.intV)){
                 AUX_Scan->_slotID++;
                 unpinPage(re->mgmtData,AUX_Scan->pHandle);
                 return RC_OK;
              //checking next slot
              }AUX_Scan->_slotID++;
          }break;
      }break;
    case OP_COMP_EQUAL:
      l=decision->args[0];
      r=decision->args[1];
      //checking all pages
      while(AUX_Scan->_sPage < AUX_Scan->_numPages){
          pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
          AUX_Scan->_recsPage=strlen(AUX_Scan->pHandle->data)/AUX_Scan->_recLength;
          //checking all records in the page
          while(AUX_Scan->_slotID < AUX_Scan->_recsPage){
              id.page=AUX_Scan->_sPage;
              id.slot=AUX_Scan->_slotID;
              //getting the record of this id
              getRecord(re,id,record);
              //getting the aattribute of the record to see if it matches
              getAttr(record,re->schema,r->expr.attrRef,cValue);
              //match found
              if((re->schema->dataTypes[r->expr.attrRef]==DT_STRING)&&(strcmp(cValue[0]->v.stringV , l->expr.cons->v.stringV)==0)){
                 AUX_Scan->_slotID++;
                 unpinPage(re->mgmtData,AUX_Scan->pHandle);
                 return RC_OK;
              //match found
              }else if((re->schema->dataTypes[r->expr.attrRef]==DT_INT)&&(cValue[0]->v.intV == l->expr.cons->v.intV)){
                  AUX_Scan->_slotID++;
                  unpinPage(re->mgmtData,AUX_Scan->pHandle);
                  return RC_OK;
              //match found
              }else if((cValue[0]->v.floatV == l->expr.cons->v.floatV)&&(re->schema->dataTypes[r->expr.attrRef]==DT_FLOAT)){
                  AUX_Scan->_slotID++;
                  unpinPage(re->mgmtData,AUX_Scan->pHandle);
                  return RC_OK;
              //checking next slot
              } AUX_Scan->_slotID++;
          }break;     
      }break; 
    case OP_BOOL_NOT:
        //using an auxiliary expresion to find a match
        auxExpr=expression->expr.op->args[0];
        decision2=auxExpr->expr.op;
        r=decision2->args[0];
        l=decision2->args[1];
        if (decision2->type==OP_COMP_SMALLER){
          //checking all pages
          while(AUX_Scan->_numPages>AUX_Scan->_sPage){
              pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
              AUX_Scan->_recsPage=strlen(AUX_Scan->pHandle->data)/AUX_Scan->_recLength;
              //checking all records in the page
              while(AUX_Scan->_slotID < AUX_Scan->_recsPage){
                  id.slot=AUX_Scan->_slotID;
                  id.page=AUX_Scan->_sPage;
                  //getting the record of this id
                  getRecord(re,id,record);
                  //getting the aattribute to see if it matches
                  getAttr(record,re->schema,r->expr.attrRef,cValue);
                  if((cValue[0]->v.intV > l->expr.cons->v.intV)&&(re->schema->dataTypes[r->expr.attrRef]==DT_INT)){
                      AUX_Scan->_slotID++;
                      unpinPage(re->mgmtData,AUX_Scan->pHandle);
                      return RC_OK;
                  //checking next slot
                  }AUX_Scan->_slotID++;
              }break; 
          }break;
        }break;
  }
  return RC_RM_NO_MORE_TUPLES;
}

//this method closes the started scan
RC closeScan (RM_ScanHandle *scan){
  AUX_Scan *auxScan=search(scan,ptr_Scan);
  return delete(scan,&ptr_Scan);
}

//This two methods should one initialize and the other shutdown 
//a record manager, but since we do not use any global structure
//it is not required to be implemented.
RC initRecordManager (void *mgmtData){return RC_OK;}
RC shutdownRecordManager (){return RC_OK;}

//This method creates a new table storing the schema 
//in the first pagethrough the previous assignment 
//buffer manager.
RC createTable (char *name, Schema *schema){
  char fname[50]={'\0'};
  //creating the page file in binary format
  strcat(fname,name);
  strcat(fname,".bin");
  createPageFile(fname);
  //using this global structure to manage everywhere without the need of 
  //read each time the schema
  mainSchema=*schema;
  return RC_OK;
}

//The method opens a table we have created through
//the previous assignment buffer manager.
RC openTable (RM_TableData *rel, char *name){
  BM_BufferPool *buffer=MAKE_POOL();
  //allocating memory for the schema
  Schema *schema=(Schema *)malloc(sizeof(Schema));
  char fname[50]={'\0'};
  strcat(fname,name);
  strcat(fname,".bin");
  //initializing buffer pool
  initBufferPool(buffer,fname,4,RS_FIFO,NULL);
  rel->name=name;
  rel->mgmtData=buffer;  
  *schema=mainSchema;
  //setting values of the opened page
  rel->schema=schema;
  return RC_OK;
}

//This method closes the table we have created 
//by shutting down the pool
RC closeTable (RM_TableData *rel){
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  shutdownBufferPool(buffer);
  free(buffer);
  return RC_OK;
}

//This method deletes the table we have created
RC deleteTable (char *name){
  char fname[50]={'\0'};
  strcat(fname,name);
  strcat(fname,".bin");
  destroyPageFile(fname);
  return RC_OK;
}

//This method returns the total number 
//of touples in a page.
int getNumTuples (RM_TableData *rel){
  int tuplesCount=0,pLength;
  PageNumber pageNum=1;
  //creating pointers to manage the data  
  BM_PageHandle *pagehandle=MAKE_PAGE_HANDLE();
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  SM_FileHandle *fHandle=(SM_FileHandle *)buffer->mgmtData;
  PageNumber numPages=fHandle->totalNumPages;
  //checking all the pages
  while(pageNum < numPages){
    pinPage(buffer,pagehandle,pageNum);
    pageNum++;
    int i;
    for(i=0;i < PAGE_SIZE;i++){
        //if a new touple is found, the count is updated
        if(tuplesCount=pagehandle->data[i]=='-') tuplesCount++;
    }
  }
  return tuplesCount;
}
