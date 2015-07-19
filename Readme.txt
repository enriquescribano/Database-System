README


Enrique Escribano


***USAGE***

To compile, just type:    make

To run the first test case(after compiled), type: ./test_assign3 and the second one, type: ./test_expr


ps.- it will remain some warnings about difference in pointers const char *const and *char. anyway, it works correctly.


***IMPLEMENTATION DESIGN***

We are implementing a record manager in this third assignment. This buffer manager has to deal with the storage manager designed in the first assignment and with the buffer manager 

Clearly, it has been the most difficult assignment because of the interaction of all previous ones. Just for this reason we have created some auxiliary classes that let us work more comfortable scaling the code.


Mainly we have two classes: 

- record_mgr.c: deals with the functions described in the assignment, and controls the performance of the record manager. It contains all the required methods asked in the assignment. So there is no much need to explain its use.

- record_scan.c: this class represents a linked list scans. For
every scan created, we keep a pointer to it in this class. So when we want to operate with one of them, we just start it with some help of another auxiliary scan to set up all features of the starting scan, and then do the operations we want with it.

Moreover, this class defines 2 new structures:

	- Scan_Node: it basically is a node that store all features of the scan into its pointer.This structure let us insert a scan when it is initialized into the linked list.The features it contains are:
		· The page as _sPage and the slot as _slotID
  		· The number of records in a page as _recsPage
  		· The length of the record as _recLength
  		· The total number of pages as _numPages

	- AUX_Scan: used by the scan to map it when it is used. It is just in charge to handle the location of the scan.

	Apart from this, as a overview of the class, it has 3 methods:
		//trivial
		-insert
		-delete
		// and
		-search

*** DESCRIPTION OF THE CODE ***

You can find appropiate comments along the source code which will help you to understand the logic.


***TEST CASES***

test_assign3_1.c  Passes OK.
test_expr.c 	  Passes OK.


