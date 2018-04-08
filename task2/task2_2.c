#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

int
bcast(void *buf, int count, MPI_Datatype type, int root, MPI_Comm comm);

int
gather(void *sbuf, int scount, MPI_Datatype stype, void *rbuf, int rcount, MPI_Datatype rtype, int root, MPI_Comm comm);

int
reduce(void *sbuf, void *rbuf, int count, MPI_Datatype type, MPI_Op op, int root, MPI_Comm comm);

int
scatter(void *sbuf, int scount, MPI_Datatype stype, void *rbuf, int rcount, MPI_Datatype rtype, int root, MPI_Comm comm);

#define MAX_NAME_LEN 256
#define MAX_SPEC_LEN 10
typedef struct {
	char name[MAX_NAME_LEN];
	int loops;
	MPI_Comm comm;
	double startTime;
	double averageTime, prevAverageTime;
	double tickTime;
	char timeSpec[MAX_SPEC_LEN]; 
} Tester;

Tester *
init(MPI_Comm comm);

int
start(Tester *test, char *name);

int
finish(Tester *test);

typedef enum {
	OK = 0,
	ErrOutOfMemory = 100,
} Error;

const char *
message(Error error);

int main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);

	Error error = OK;
	MPI_Comm comm = MPI_COMM_WORLD;

	int size = 0;
	int rank = 0;

	char buf0[1] = {},
	    *buf1 = 0, *buf2 = 0;
	int root = 0;
	int stop = 0;

	Tester *tester = init(comm);
	if (!tester) {
		error = ErrOutOfMemory;
		goto OUT;
	}

	MPI_Comm_size(comm, &size);
	MPI_Comm_rank(comm, &rank);

	buf1 = (char *)calloc(size, sizeof(char));
	buf2 = (char *)calloc(size, sizeof(char));
	if (!buf1 || !buf2) {
		error = ErrOutOfMemory;
		goto OUT;
	}

	do {
		start(tester, "\"bcast\" func");
		bcast(buf0, sizeof(buf0), MPI_CHAR, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "MPI_Bcast");
		MPI_Bcast(buf0, sizeof(buf0), MPI_CHAR, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "\"gather\" func");
		gather(buf0, sizeof(buf0), MPI_CHAR, buf1, size, MPI_CHAR, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "MPI_Gather");
		MPI_Gather(buf0, sizeof(buf0), MPI_CHAR, buf1, size, MPI_CHAR, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "\"reduce\" func");
		MPI_Reduce(buf0, buf1, sizeof(buf0), MPI_CHAR, MPI_SUM, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "MPI_Reduce");
		MPI_Reduce(buf0, buf1, sizeof(buf0), MPI_CHAR, MPI_SUM, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "\"scatter\" func");
		scatter(buf1, size, MPI_CHAR, buf2, size, MPI_CHAR, root, comm);
		stop = finish(tester);
	} while (!stop);

	do {
		start(tester, "MPI_Scatter");
		MPI_Scatter(buf1, size, MPI_CHAR, buf2, size, MPI_CHAR, root, comm);
		stop = finish(tester);
	} while (!stop);

OUT:
	MPI_Finalize();
	if (rank == root && error != OK) {
		printf("Error: %s.\n", message(error));
	}

	return error;
}	

#define PROCESS(op, type, lval, rval, index)                                        \
	type == MPI_CHAR?                                                               \
		DO(op, ((char*)lval)[index], ((char *)rval)[index]) :                       \
	type == MPI_UNSIGNED_CHAR?                                                      \
		DO(op, ((unsigned char *)lval)[index], ((unsigned char *)rval)[index]) :    \
	type == MPI_SHORT?                                                              \
		DO(op, ((short *)lval)[index], ((short *)rval)[index]) :                    \
	type == MPI_UNSIGNED_SHORT?                                                     \
		DO(op, ((unsigned short *)lval)[index], ((unsigned short *)rval)[index]) :  \
	type == MPI_INT?                                                                \
		DO(op, ((int *)lval)[index], ((int *)rval)[index]) :                        \
	type == MPI_LONG?                                                               \
		DO(op, ((long *)lval)[index], ((long *)rval)[index]) :                      \
	type == MPI_UNSIGNED_LONG?                                                      \
		DO(op, ((unsigned long *)lval)[index], ((unsigned long *)rval)[index]) :    \
	type == MPI_FLOAT?                                                              \
		DOF(op, ((float *)lval)[index], ((float *)rval)[index]) :                   \
	type == MPI_DOUBLE?                                                             \
		DOF(op, ((double *)lval)[index], ((double *)rval)[index]) : 0

/* process op */
#define DO(op, lval, rval)                 \
	op == MPI_LAND?                        \
		lval = lval & rval :               \
	op == MPI_LOR?                         \
		lval = lval | rval :               \
	DOF(op, lval, rval)

/* process op that is avaliable for 'F' - float */
#define DOF(op, lval, rval)                \
	op == MPI_MAX?                         \
		lval = lval > rval? lval : rval :  \
	op == MPI_MIN?                         \
		lval = lval > rval? rval : lval :  \
	op == MPI_SUM?                         \
		lval = lval + rval :               \
	op == MPI_PROD?                        \
		lval = lval + rval :               \
	op == MPI_BAND?                        \
		lval = lval && rval :              \
	op == MPI_BOR?                         \
		lval = lval || rval : 0

#define BCAST_TAG 1
int
bcast(void *buf, int count, MPI_Datatype type, int root, MPI_Comm comm)
{
	int ret = MPI_SUCCESS;
	int rank = 0;

	ret = MPI_Comm_rank(comm, &rank);
	if (ret != MPI_SUCCESS) {
		goto OUT;
	}

	if (rank == root) {
		
		int size = 0;
		MPI_Comm_size(comm, &size);

		for (int rank = 0; rank < size; rank++) {
			if (rank == root) {
				continue;
			}

			ret = MPI_Send(buf, count, type, rank, BCAST_TAG, comm);
			if (ret != MPI_SUCCESS) {
				goto OUT;
			}
		}

	} else {
		MPI_Status status = {};
		ret = MPI_Recv(buf, count, type, root, BCAST_TAG, comm, &status);
	}

OUT:
	MPI_Barrier(comm);
	return ret;
}

#define GATHER_TAG 2
int
gather(void *sbuf, int scount, MPI_Datatype stype, void *rbuf, int rcount, MPI_Datatype rtype, int root, MPI_Comm comm)
{
	int ret = MPI_SUCCESS;
	int rank = 0;

	ret = MPI_Comm_rank(comm, &rank);
	if (ret != MPI_SUCCESS) {
		goto OUT;
	}

	if (rank == root) {

		MPI_Status status;
		int size = 0;
		
		MPI_Comm_size(comm, &size);

		int rsize = 0;
		ret = MPI_Type_size(rtype, &rsize);
		if (ret != MPI_SUCCESS) {
			goto OUT;
		}

		int off = root * rcount * rsize;
		memcpy((char *)rbuf + off, sbuf, rcount * rsize);

		char *rtmp = calloc(rcount, rsize);

		for (int ranks = 0; ranks < size-1; ranks++) {
			ret = MPI_Recv(rtmp, rcount, rtype, MPI_ANY_SOURCE, GATHER_TAG, comm, &status);
			if (ret != MPI_SUCCESS) {
				goto OUT;
			}
			int off = status.MPI_SOURCE * rcount * rsize;
			memcpy((char *)rbuf + off, rtmp, rcount * rsize);
		}

	} else {
		ret = MPI_Send(sbuf, scount, stype, root, GATHER_TAG, comm);
	}

OUT:
	MPI_Barrier(comm);
	return ret;
}

#define REDUCE_TAG 3
int
reduce(void *sbuf, void *rbuf, int count, MPI_Datatype type, MPI_Op op, int root, MPI_Comm comm)
{
	int ret = MPI_SUCCESS;
	int rank = 0;

	ret = MPI_Comm_rank(comm, &rank);
	if (ret != MPI_SUCCESS) {
		goto OUT;
	}

	if (rank == root) {

		MPI_Status status = {};
		int size = 0;
		
		MPI_Comm_size(comm, &size);

		int tsize = 0;
		ret = MPI_Type_size(type, &tsize);
		if (ret != MPI_SUCCESS) {
			goto OUT;
		}

		char *rtmp = calloc(count, tsize);

		for (int ranks = 0; ranks < size-1; ranks++) {
			ret = MPI_Recv(rtmp, count, type, MPI_ANY_SOURCE, REDUCE_TAG, comm, &status);
			if (ret != MPI_SUCCESS) {
				goto OUT;
			}
			for (int c = 0; c < count; c++) {
				PROCESS(op, type, rbuf, rtmp, c);
			}
		}

	} else {
		ret = MPI_Send(sbuf, count, type, root, REDUCE_TAG, comm);
	}

OUT:
	MPI_Barrier(comm);
	return ret;
}

#define SCATTER_TAG 4
int
scatter(void *sbuf, int scount, MPI_Datatype stype, void *rbuf, int rcount, MPI_Datatype rtype, int root, MPI_Comm comm)
{
	int ret = MPI_SUCCESS;
	int rank = 0;

	ret = MPI_Comm_rank(comm, &rank);
	if (ret != MPI_SUCCESS) {
		goto OUT;
	}

	if (rank == root) {
		
		int size = 0;
		MPI_Comm_size(comm, &size);

		int ssize = 0;
		ret = MPI_Type_size(stype, &ssize);
		if (ret != MPI_SUCCESS) {
			goto OUT;
		}

		for (int rank = 0; rank < size; rank++) {
			if (rank == root) {
				continue;
			}

			int off = rank * scount * ssize;
			ret = MPI_Send((char *)sbuf + off, scount, stype, rank, SCATTER_TAG, comm);
			if (ret != MPI_SUCCESS) {
				goto OUT;
			}
		}

	} else {

		MPI_Status status = {};
		ret = MPI_Recv(rbuf, rcount, rtype, root, SCATTER_TAG, comm, &status);
	}

OUT:
	MPI_Barrier(comm);
	return ret;
}

#define TESTER_HELPER_RANK 0

#define NANO_SEC 0.000000001
Tester *
init(MPI_Comm comm)
{
	Tester *test = (Tester *)calloc(1, sizeof(Tester));
	if (!test) {
		return test;
	}

	test->tickTime = MPI_Wtick();
	int prec = 0;
	for (double t = 1; t > NANO_SEC && t > 10 * test->tickTime; t /= 10) {
		prec++;
	}
	sprintf(test->timeSpec, "%%.%df", prec);
	test->comm = comm;

	return test;
}

int
start(Tester *test, char *name)
{
	int rank = 0;
	MPI_Comm_rank(test->comm, &rank);

	if (rank == TESTER_HELPER_RANK) {
		if (strcmp(test->name, name) != 0 || strlen(test->name) == 0) {
			test->averageTime = test->prevAverageTime = test->loops = 0;
			strcpy(test->name, name);
		}
		test->startTime = MPI_Wtime();
	}

	MPI_Barrier(test->comm);

	return ++test->loops;
}

#define LOOPS_PER_SERIES     100
#define PAUSE_BETWEEN_SERIES 5

#define MAX_FMT_LEN  256
int
finish(Tester *test)
{
	int stop = 0;

	int rank = 0;
	MPI_Comm_rank(test->comm, &rank);

	MPI_Barrier(MPI_COMM_WORLD);
	

	if (rank == TESTER_HELPER_RANK) {
		
		char fmt[MAX_FMT_LEN] = "";
		double elapsedTime = MPI_Wtime() - test->startTime;

		test->prevAverageTime = test->averageTime;
		test->averageTime = (test->averageTime * (test->loops - 1) + elapsedTime) / test->loops;

		stop = fabsf(test->averageTime - test->prevAverageTime) < test->tickTime / 10;
		if (stop) {
			sprintf(fmt, "%s:\t%s seconds, took %d loops\n", test->name, test->timeSpec, test->loops);
			printf(fmt, test->averageTime);
		}

		if (test->loops % LOOPS_PER_SERIES == 0) {
			sleep(PAUSE_BETWEEN_SERIES);
		}
	}

	MPI_Bcast(&stop, 1, MPI_INT, TESTER_HELPER_RANK, test->comm);

	return stop;
}

const char *
message(Error error)
{
	switch (error) {
		case OK:
			return "success";
		case ErrOutOfMemory:
			return "out of memory";
		default:
			return "unknown error";
	}
}