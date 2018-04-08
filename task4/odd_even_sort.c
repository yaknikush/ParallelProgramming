#include "odd_even_sort.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <unistd.h>

#include <stdio.h>

#define COMPARE_IMPLEMENTATION(T)           \
int                                         \
compare_##T(const void *x, const void *y)   \
{                                           \
	T z = *(const T *)x - *(const T *)y;    \
	return (z > 0) - (z < 0);               \
}

COMPARE_IMPLEMENTATION(char)
COMPARE_IMPLEMENTATION(short)
COMPARE_IMPLEMENTATION(int)
COMPARE_IMPLEMENTATION(long)
COMPARE_IMPLEMENTATION(double)
COMPARE_IMPLEMENTATION(float)
#undef COMPARE_IMPLEMENTATION

#define MAX_ELEM_SIZE 1024

static void
swap_elems(int elem_size, void *x_elem, void *y_elem)
{
	char buf[MAX_ELEM_SIZE];
	void *tmp_elem = (void *)buf;

	memmove(tmp_elem, x_elem, elem_size);
	memmove(x_elem, y_elem, elem_size);
	memmove(y_elem, tmp_elem, elem_size);
}

static void
compare_swap_elems(void *elems, int elem_size, compare_func compare_elems, int idx_1, int idx_2)
{
	int idx[2] = {idx_1, idx_2}; 

	void *ptr[2];
	for (int n = 0; n < 2; n++) {
		ptr[n] = (void *)((char *)elems + idx[n] * elem_size);
	}

	if (compare_elems(ptr[0], ptr[1]) > 0) {
		swap_elems(elem_size, ptr[0], ptr[1]);
	}
}

static void
bubble_sort(void *elems, int n_elems, int elem_size, compare_func compare_elems)
{
	for (int n = 1; n <= n_elems; n++) {
		for (int j = 0; j < n_elems - n; j++) {
			compare_swap_elems(elems, elem_size, compare_elems, j, j+1);
		}
	}
}

typedef struct {
	void *elems;
	int n_elems;
	int elem_size;
	compare_func compare_elems;
	
	int from_even, to_even;
	int from_odd, to_odd;

	pthread_barrier_t *barrier;
} task_t;

#define NUMBER_OF_THREADS   8
#define EASY_NUMBER_OF_ELEMENTS 20

void *
do_sort_task(void *arg)
{
	task_t *task = (task_t *)(arg);
	int from = 0, to = 0;

	int n_loops = task->n_elems;

	for (int n = 0; n <= n_loops; n++) {

		pthread_barrier_wait(task->barrier);

		if (n % 2 == 0) {
			from = task->from_even, to = task->to_even;
		} else {
			from = task->from_odd, to = task->to_odd;
		}

		for (int i 	= from; i < to-1; i += 2) {
			compare_swap_elems(task->elems, task->elem_size, task->compare_elems, i, i+1);
		}

		compare_swap_elems(task->elems, task->elem_size, task->compare_elems, to-1, to);
	}

	return 0;
}

#define min(x, y) ((x) < (y))? (x) : (y)

void
odd_even_sort(void *elems, int n_elems, int elem_size, compare_func compare_elems)
{
	assert(elem_size <= MAX_ELEM_SIZE);

	if (n_elems < EASY_NUMBER_OF_ELEMENTS) {
		bubble_sort(elems, n_elems, elem_size, compare_elems);
		return;
	}

	pthread_t threads[NUMBER_OF_THREADS];
	pthread_barrier_t thread_barrier;
	pthread_barrier_init(&thread_barrier, NULL, NUMBER_OF_THREADS);

	int n_elems_per_task = n_elems / NUMBER_OF_THREADS;
	task_t tasks[NUMBER_OF_THREADS];
	
	int *ret = 0;

	for (int n = 0; n < NUMBER_OF_THREADS; n++) {

		bool is_last = (n == NUMBER_OF_THREADS-1);

		task_t task = {
			.elems = elems,
			.n_elems = n_elems,
			.elem_size = elem_size,
			.compare_elems = compare_elems,
			
			.from_even = n_elems_per_task * n,
			.to_even = is_last? n_elems : n_elems_per_task * (n+1),

			.from_odd = 1 + n_elems_per_task * n,
			.to_odd = is_last? n_elems : 1 + n_elems_per_task * (n+1),

			.barrier = &thread_barrier
		};

		memcpy(&tasks[n], &task, sizeof(task_t));
		pthread_create(&threads[n], NULL, do_sort_task, &tasks[n]);
	}

	for (int n = 0; n < NUMBER_OF_THREADS; n++) {
		pthread_join(threads[n], (void *)(&ret));
	}
}

#undef min