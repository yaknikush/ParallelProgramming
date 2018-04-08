#include "odd_even_merge_sort.h"
#include "odd_even_sort.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <mpi.h>

#define DBG printf("here\n");

#define MPIDBG                                   \
do {                                             \
	int proc_rank = 0;                           \
	MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);   \
	(proc_rank == 0)? printf("here\n") : 0;      \
} while (1);

char *
error_message(int err)
{
	switch(err) {
	case OK:
		return "ok";

	case ErrMPI:
		return "internal MPI error";

	case ErrInvalidType:
		return "invalid type";

	case ErrOutOfMemory:
		return "out of memory";

	default:
		return "unknown error";
	}
}

static compare_func
compare_type(MPI_Datatype type)
{
	switch (type) {
	case MPI_CHAR:
		return compare_char;

	case MPI_SHORT:
		return compare_short;

	case MPI_INT:
		return compare_int;

	case MPI_LONG:
		return compare_long;

	case MPI_FLOAT:
		return compare_float;

	case MPI_DOUBLE:
		return compare_double;

	default:
		return NULL;
	}
}

static int
merge_sort(void *left_buf, int left_count,
           void *right_buf, int right_count, int size, compare_func comp)
{
	int index = 0,
	    left_index = 0,
	    right_index = 0;

	void *tmp_buf = calloc(left_count + right_count, size);
	if (tmp_buf == NULL) {
		return ErrOutOfMemory;
	}

	void *tmp_ptr = tmp_buf,
	     *left_ptr = left_buf,
	     *right_ptr = right_buf;

	int rank = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	while (1) {

		if (left_index == left_count)  {
			memcpy(tmp_ptr, right_ptr, (right_count - right_index) * size);
			break;
		} 
		if (right_index == right_count) {
			memcpy(tmp_ptr, left_ptr, (left_count - left_index) * size);
			break;
		}

		if (comp(left_ptr, right_ptr) < 0) {
			memcpy(tmp_ptr, left_ptr, size);
			left_ptr = (void *)((char *)left_ptr + size);
			left_index++;
		} else {
			memcpy(tmp_ptr, right_ptr, size);
			right_ptr = (void *)((char *)right_ptr + size);
			right_index++;
		}

		tmp_ptr = (void *)((char *)tmp_ptr + size);
		index++;
	}

	tmp_ptr = tmp_buf;
	memcpy(left_buf, tmp_ptr, left_count * size);
	
	tmp_ptr = (void *)((char *)tmp_ptr + left_count * size);
	memcpy(right_buf, tmp_ptr, right_count * size);

	free(tmp_buf);

	return OK;
}

#define max(x, y) ((x) > (y))? (x) : (y)
#define min(x, y) ((x) < (y))? (x) : (y)

#define is_even(n) ((n) % 2 == 0)
#define is_odd(n)  ((n) % 2 == 1)

#define ODD_EVEN_MERGE_SORT_TAG 1
#define ODD_EVEN_SORT_SIZE      2

int
odd_even_merge_sort(void *buf, int count, MPI_Datatype type, int root_rank, MPI_Comm comm)
{
	int err = OK;

	int n_proc = 0;
	int ret = MPI_Comm_size(comm, &n_proc);
	if (ret != MPI_SUCCESS) {
		return ErrMPI;
	}

	int proc_rank = 0;
	ret = MPI_Comm_rank(comm, &proc_rank);
	if (ret != MPI_SUCCESS) {
		return ErrMPI;
	}

	int size = 0;
	ret = MPI_Type_size(type, &size);
	if (ret != MPI_SUCCESS) {
		return ErrMPI;
	}

	compare_func comp = compare_type(type);
	if (comp == NULL) {
		return ErrInvalidType;
	}

	if (count < n_proc * 3) {
		if (proc_rank == root_rank) {
			odd_even_sort(buf, count, size, comp);
		}
		MPI_Barrier(comm);
		return OK;
	}

	int proc_count = count / n_proc;
	int root_count = count - (n_proc-1) * proc_count;

	proc_count = (proc_rank == root_rank)? root_count : proc_count;
	MPI_Barrier(comm);

	void *proc_buf = calloc(proc_count, size);
	if (proc_buf == NULL) {
		return ErrOutOfMemory;
	}

	void *neighbor_buf = calloc(proc_count, size);
	if (neighbor_buf == NULL) {
		return ErrOutOfMemory;
	}

	MPI_Barrier(comm);
	int *buf_counts = NULL, *buf_offsets = NULL;
	if (proc_rank == root_rank) {

		buf_counts = (int *)calloc(proc_count, size);
		if (buf_counts == NULL) {
			return ErrOutOfMemory;
		}

		buf_offsets = (int *)calloc(proc_count, size);
		if (buf_offsets == NULL) {
			return ErrOutOfMemory;
		}

		int offset = 0;
		for (int n = 0; n < n_proc; n++) {
			buf_counts[n] = (n == 0)? root_count : count / n_proc;
			buf_offsets[n] = (offset += (n == 0)? 0 : buf_counts[n-1]);
		}
	}

	ret = MPI_Scatterv(buf, buf_counts, buf_offsets, type, proc_buf, proc_count, type, root_rank, comm);
	if (ret != MPI_SUCCESS) {
		return ErrMPI;
	}
	
	odd_even_sort(proc_buf, proc_count, size, comp);

	int n_loops = n_proc;

	for (int n = 0; n < n_loops; n++) {

		int neighbor_rank = 0;
		if ((is_even(n) && is_even(proc_rank)) ||
			(is_odd(n)  && is_odd(proc_rank))) {
			neighbor_rank = min(proc_rank + 1, n_proc - 1);
		} else {
			neighbor_rank = max(proc_rank - 1, 0);
		}

		if (neighbor_rank > proc_rank) {

			ret = MPI_Send(proc_buf, proc_count, type, neighbor_rank, ODD_EVEN_MERGE_SORT_TAG, comm);
			if (ret != MPI_SUCCESS) {
				return ErrMPI;
			}

			ret = MPI_Recv(proc_buf, proc_count, type, neighbor_rank, ODD_EVEN_MERGE_SORT_TAG, comm, MPI_STATUS_IGNORE);
			if (ret != MPI_SUCCESS) {
				return ErrMPI;
			}

		} else if (neighbor_rank < proc_rank) {
			
			int neighbor_count = (neighbor_rank == root_rank)? root_count : proc_count;
			
			ret = MPI_Recv(neighbor_buf, neighbor_count, type, neighbor_rank,
			               ODD_EVEN_MERGE_SORT_TAG, comm, MPI_STATUS_IGNORE);
			if (ret != MPI_SUCCESS) {
				return ErrMPI;
			}

			err = merge_sort(neighbor_buf, neighbor_count,
				             proc_buf, neighbor_count, size, comp);
			if (err != OK) {
				return err;
			}

			ret = MPI_Send(neighbor_buf, neighbor_count, type, neighbor_rank,
			               ODD_EVEN_MERGE_SORT_TAG, comm);
			if (ret != MPI_SUCCESS) {
				return ErrMPI;
			}
		}
	
		ret = MPI_Barrier(comm);
		if (ret != MPI_SUCCESS) {
			return ErrMPI;
		}
	}

	ret = MPI_Gatherv(proc_buf, proc_count, type, buf, buf_counts, buf_offsets, type, root_rank, comm);
	if (ret != MPI_SUCCESS) {
		return ErrMPI;
	}

	MPI_Barrier(comm);

	free(proc_buf);
	free(neighbor_buf);
	
	if (proc_rank == root_rank) {
		free(buf_counts);
		free(buf_offsets);
	}

	return OK;
}

#undef max
#undef min
#undef is_even
#undef is_odd