#include <mpi.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>

#define MAX_LEN_TO_PRINT	1 << 8
#define MASTER_RANK         0
#define TASK_MSG            0

typedef struct {
	char print[MAX_LEN_TO_PRINT];
	bool done;
	int checksum;
} Task;

void
InitTask(Task *t, int rank);

void
DoTask(Task *t);

bool
TaskIsOK(Task *t);

int
CalcTaskChecksum(Task *t);

typedef enum {
	OK             = 0,
	ErrTaskNotOk   = 1,
	ErrTaskNotDone = 2,
} Error;

typedef MPI_Status Status;

void
PrintError(Error err);

void
PrintTask(Task *t);

int
main(int argc, char *argv[])
{
	int res = 0;

	MPI_Init(&argc, &argv);
	Error err = OK;

	int comm = MPI_COMM_WORLD,
	    numOfProcesses = 0,
	    rank = 0;

	MPI_Comm_size(comm, &numOfProcesses);
	MPI_Comm_rank(comm, &rank);

	char b[sizeof(Task)];
	for (int i = 0; i < sizeof(Task); i++)
		b[i] = 0;

	Task *t = (Task*)b;
	Status status;	

	if (rank != 0) {
	
		MPI_Recv(b, sizeof(Task), MPI_CHAR, rank-1, TASK_MSG, comm, &status);

		bool ok = TaskIsOK(t);
		if (ok) {
			DoTask(t);
			err = t->done? OK : ErrTaskNotDone;
		} else {
			err = ErrTaskNotOk;
		}

		if (err != OK) {
			PrintError(err);
			MPI_Abort(comm, MPI_ERR_OTHER);
		}
	}
	
	if (rank != numOfProcesses-1) {

		InitTask(t, rank+1);
		MPI_Send(b, sizeof(Task), MPI_CHAR, rank+1, TASK_MSG, comm);
	}

	MPI_Finalize();
	
	return 0;
}

void
InitTask(Task *t, int rank)
{
	int p = sprintf(t->print, "%d\n", rank);
	t->done = false;
	t->checksum = CalcTaskChecksum(t);

	assert(p < MAX_LEN_TO_PRINT);
}

void
DoTask(Task *t)
{
	int expected = strlen(t->print);
	int got = write(1, t->print, expected);

	t->done = (got == expected);
	t->checksum = CalcTaskChecksum(t);
}

bool
TaskIsOK(Task *t)
{
	int expected = t->checksum;
	int got = CalcTaskChecksum(t);

	return got == expected;
}

int
CalcTaskChecksum(Task *t)
{
	int cs = 1 << 10;

	char *b = (char*)t;
	for (int i = 0; i < sizeof(Task)-sizeof(int); i++)
		cs ^= (int)b[i];

	return cs;
}

void
PrintError(Error err)
{
	switch (err) {
		case ErrTaskNotOk:
			printf("Error: failed while doing the task.\n");
			break;

		case ErrTaskNotDone:
			printf("Error: got invalid task after send-recv ops.\n");
			break;
	}
}