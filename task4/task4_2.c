#include "odd_even_merge_sort.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <time.h>
#include <mpi.h>

int *
generate_elems(int n_elems);

void
free_elems(int *elems);

bool
is_sorted_elems(int *elems, int n_elems);

int
main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);

	MPI_Comm comm = MPI_COMM_WORLD;

	int rank = 0, root = 0;
	MPI_Comm_rank(comm, &rank);

	int n_elems[] = {20, 1024, 1024 * 16};
	int *elems = NULL;

	for (int i = 0; i < sizeof(n_elems) / sizeof(*n_elems); i++) {

		if (rank == root) {
			elems = generate_elems(n_elems[i]);
			if (elems == NULL) {
				printf("Error: couldn't generate enough elements (%d).\n", n_elems);
				return 1;
			}

			printf("=== RUN  Test on %d elements\n", n_elems[i]);
		}

		time_t start_time = clock();
		odd_even_merge_sort(elems, n_elems[i], MPI_INT, root, comm);
		time_t elapsed_time = clock() - start_time;


		if (rank == root) {
			
			printf("(elapsed time %d clocks)\n", elapsed_time);

			if (!is_sorted_elems(elems, n_elems[i])) {
				printf("=== FAIL Test on %d elements\n", n_elems[i]);
				return 1;
			}

			printf("=== PASS Test on %d elements\n", n_elems[i]);

			free_elems(elems);
		}
	}

	MPI_Finalize();

	return 0;
}

#define MAX_ELEMENT 100

int *
generate_elems(int n_elems)
{
	int *elems = (int *)calloc(n_elems, sizeof(int));
	if (elems == NULL) {
		return NULL;
	}

	for (int n = 0; n < n_elems; n++) {
		elems[n] = rand() % MAX_ELEMENT;
	}

	return elems;
}

void
free_elems(int *elems)
{
	free(elems);
}

void
print_elems(int *elems, int n_elems)
{
	printf("[");
	for (int n = 0; n < n_elems; n++){
		printf(" %d", elems[n]);
	}
	printf(" ]\n");
}

bool
is_sorted_elems(int *elems, int n_elems)
{
	if (n_elems == 0) {
		return true;
	}

	int e = elems[0];
	for (int n = 0; n < n_elems; n++) {
		if (e > elems[n]) {
			print_elems(elems, n_elems);
			printf("Invalid element with index %d 8===> [... %d %d %d ...]!\n",
				n,
				(n != 0)? elems[n-1] : -1,
				elems[n],
				(n != n_elems-1)? elems[n+1] : -1);
			return false;
		}
		e = elems[n];
	}

	return true;
}