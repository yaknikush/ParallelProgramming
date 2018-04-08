#ifndef __ODD_EVEN_MERGE_SORT__
#define __ODD_EVEN_MERGE_SORT__

#include <mpi.h>

typedef enum {
	OK             = 1,
	ErrMPI         = 2,
	ErrInvalidType = 3,
	ErrOutOfMemory = 4
} error_t;

char *
error_message(int err);

int
odd_even_merge_sort(void *buf, int count, MPI_Datatype type, int root_rank, int comm);

#endif