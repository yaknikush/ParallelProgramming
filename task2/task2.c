#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

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

	Tester* test = init(comm);
	if (!test) {
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
		start(test, "MPI_Bcast");
		MPI_Bcast(buf0, sizeof(buf0), MPI_CHAR, root, comm);
		stop = finish(test);
	} while (!stop);

	do {
		start(test, "MPI_Gather");
		MPI_Gather(buf0, sizeof(buf0), MPI_CHAR, buf1, size, MPI_CHAR, root, comm);
		stop = finish(test);
	} while (!stop);

	do {
		start(test, "MPI_Reduce");
		MPI_Reduce(buf0, buf1, sizeof(buf0), MPI_CHAR, MPI_SUM, root, comm);
		stop = finish(test);
	} while (!stop);

	do {
		start(test, "MPI_Scatter");
		MPI_Scatter(buf1, size, MPI_CHAR, buf2, size, MPI_CHAR, root, comm);	
		stop = finish(test);
	} while (!stop);

OUT:
	MPI_Finalize();
	if (rank == root && error != OK) {
		printf("Error: %s.\n", message(error));
	}

	return error;
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
#define PAUSE_BETWEEN_SERIES 3

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