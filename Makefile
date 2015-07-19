all: test_assign3 test_expr  clean

test_assign3: test_assign3_1.o expr.o dberror.o record_mgr.o record_scan.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o buffer_list.o
			  cc -o test_assign3 test_assign3_1.o expr.o dberror.o record_mgr.o record_scan.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o buffer_list.o

test_expr:  test_expr.o expr.o dberror.o record_mgr.o record_scan.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o buffer_list.o
			cc -o test_expr test_expr.o expr.o dberror.o record_mgr.o record_scan.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o buffer_list.o

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h
			 cc -c test_expr.c

clean:
	-rm *.o
