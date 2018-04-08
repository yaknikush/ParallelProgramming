#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>
#include <errno.h>
#include <mpi.h>

#define ERROR_TYPE 100

typedef enum {
	OK = 0,
	ErrInternal    = 1,
	ErrFileFormat  = 101,
	ErrDiffNumLen  = 102,
	ErrUnknown     = 103,
	ErrOutOfMemory = 104,
	ErrErrno       = 2,
	ErrUnistdOpen  = 201,
	ErrUnistdRead  = 202,
	ErrUnistdWrite = 203,
	ErrMpi         = 3,
	ErrMpiSend     = 301,
	ErrMpiRecv     = 302,
} error_t;

#define MAX_MESSAGE_SIZE 1024

void
print_message(error_t err);

void
internal_message(char *msg, error_t err);

void
errno_message(char *msg, error_t err);

void
mpi_message(char *msg, error_t err);

#define CHARS_PER_DIGIT 2
#define MAX_DIGIT_VALUE 99

typedef unsigned char digit_t;

digit_t
add_digits(digit_t digit1, digit_t digit2, digit_t *transfer_digit);

bool
digit_to_str(char *str, digit_t digit);

bool
str_to_digit(digit_t *digit, char *str);

#define DIGITS_PER_POOL 1024

typedef struct {
	digit_t digits[DIGITS_PER_POOL];
	int n_popped_digits;
	int n_pushed_digits;
} digits_pool_t;

error_t
new_digits_pool(digits_pool_t **pool);

error_t
read_digits(int fd, digits_pool_t *pool, int *n_digits);

error_t
write_digits(int fd, digits_pool_t *pool);

int
push_digits(digits_pool_t *pool, digit_t* digits, int n_digits);

int
pop_digits(digits_pool_t *pool, digit_t* digits, int n_digits);

bool
pool_is_empty(digits_pool_t *pool);

bool
pool_is_full(digits_pool_t *pool);

void
___clear_digits(digits_pool_t *pool);

void
___dump_digits(const char *prefix, digits_pool_t *pool);

#define DIGITS_PER_TASK 128 * 4 * 4
#define MAX_DIGITS_PER_TASK 256 * 4 * 4

typedef struct task_result {
	struct task_result* next;
	int order;
	digit_t a_digits[MAX_DIGITS_PER_TASK];
	digit_t b_digits[MAX_DIGITS_PER_TASK];
	digit_t sum_digits_v0[MAX_DIGITS_PER_TASK];
	digit_t sum_digits_v1[MAX_DIGITS_PER_TASK];
	digit_t transfer_digit_v0;
	digit_t transfer_digit_v1;
	int n_digits;
} task_result_t;

typedef struct {
	double send_time;
	double recv_time;
	task_result_t result;
} task_t;

error_t
new_task(task_t **task);

error_t
calc_trust_coef(const int rank, task_t* task, double *trust_coef);

#define TASK_TAG 1

error_t
send_task(const int rank, task_t *task);

error_t
recv_task(int *rank, task_t *task);

error_t
give_task(digits_pool_t *a_digits, digits_pool_t *b_digits, const int worker_rank, task_t *task);

error_t
collect_task_result(task_result_t **results, task_t *task);

void
merge_task_results(task_result_t **results, digits_pool_t *sum_digits);

void
do_task(task_t *task);

bool
task_is_empty(task_t *task);	

void
___clear_tasks_results(task_result_t **results);

void
___dump_task(const char *prefix, task_t *task);

void
___dump_task_results(const char *prefix, task_result_t *results);

typedef struct {
	double average_time_on_task;
	int n_tasks;
} work_stat_t;

void
update_work_stat(work_stat_t* stat, double new_time_on_task);

typedef enum {
	DYNAMIC_MODE = 1,
	STATIC_MODE  = 2
} run_mode_t;

/* default is dynamic mode */
run_mode_t mode = DYNAMIC_MODE;

void
handle_args(int argc, char *argv[]);

void
help();

int
main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);
	
	double start = MPI_Wtime();
	
	if (argc == 2) {
		if (strcmp(argv[1], "--static") == 0 || strcmp(argv[1], "-s") == 0) {
			mode = STATIC_MODE;
		} else if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
			help();
		}
	}

	error_t err = OK;

	const char *a_number_fpath = "a_number",
               *b_number_fpath = "b_number",
               *sum_fpath = "sum";

    int a_fd = 0,
        b_fd = 0,
        sum_fd = 0;

	int rank = 0,
	    size = 0;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	digits_pool_t *a_digits = NULL,      /* "a" number digits buffer */
	              *b_digits = NULL,      /* "b" number digits buffer */
	              *sum_digits = NULL;    /* final sum considering scenary pool */

	int n_a_digits = 0,
	    n_b_digits = 0,
	    n_digits = 0;

	task_t *task = NULL,
	       *empty_task = NULL;

	int n_given_tasks = 0,
	    n_done_tasks = 0;

	int worker_rank = rank,
	    manager_rank = 0;

	task_result_t* task_results = NULL;

	double trust_coef = 1.0; /* coefficient of trust to worker,
	                            multiplied on DIGITS_PER_PROCESS is equal to how much digits will be given to worker */

	err = new_task(&task);
	if (err != OK) {
		goto ERROR;
	}

	err = new_task(&empty_task);
	if (err != OK) {
		goto ERROR;
	}

	if (rank == manager_rank) {

		err = new_digits_pool(&a_digits);
		if (err != OK) {
			goto ERROR;
		}

		err = new_digits_pool(&b_digits);
		if (err != OK) {
			goto ERROR;
		}

		err = new_digits_pool(&sum_digits);
		if (err != OK) {
			goto ERROR;
		}

		a_fd = open(a_number_fpath, O_RDONLY, 0664);
		if (a_fd < 0) {
			err = ErrUnistdOpen;
			goto ERROR;
		}

		b_fd = open(b_number_fpath, O_RDONLY, 0664);
		if (a_fd < 0) {
			err = ErrUnistdOpen;
			goto ERROR;
		}

		sum_fd = open(sum_fpath, O_WRONLY | O_CREAT | O_TRUNC, 0664);
		if (sum_fd < 0) {
			err = ErrUnistdOpen;
			goto ERROR;
		}

		do {
			/* reading "b" number digits from file */
			err = read_digits(a_fd, a_digits, &n_a_digits);
			if (err != OK) {
				goto ERROR;
			}

			/* reading "a" number digits from file */
			err = read_digits(b_fd, b_digits, &n_b_digits);
			if (err != OK) {
				goto ERROR;
			}

			if (n_a_digits != n_b_digits) {
				err = ErrDiffNumLen;
				goto ERROR;
			}

			n_digits = n_a_digits = n_b_digits;

			for (int worker_rank = 1; worker_rank < size; worker_rank++) {

				err = give_task(a_digits, b_digits, worker_rank, n_digits == 0? task : empty_task);
				if (err != OK) {
					goto ERROR;
				}

				if (!task_is_empty(task)) {
					n_given_tasks++;
				}

				err = send_task(worker_rank, task);
				if (err != OK) {
					goto ERROR;
				}
			}

			while (n_done_tasks != n_given_tasks) {

				int was_worker_rank = 0;
				err = recv_task(&worker_rank, task);
				if (err != OK) {
					goto ERROR;
				}
				
				if (!task_is_empty(task)) {
					
					n_done_tasks++;

					err = collect_task_result(&task_results, task);
					if (err != OK) {
						goto ERROR;
					}
				}
				
				err = give_task(a_digits, b_digits, worker_rank, task);
				if (err != OK) {
					goto ERROR;
				}

				if (!task_is_empty(task)) {

					n_given_tasks++;

					err = send_task(worker_rank, task);
					if (err != OK) {
						goto ERROR;
					}
				}
			}

			merge_task_results(&task_results, sum_digits);

			err = write_digits(sum_fd, sum_digits);
			if (err != OK) {
				goto ERROR;
			}

		} while (n_digits != 0);

		printf("took %.0fK ticks to do it!\n", (MPI_Wtime() - start) / (MPI_Wtick() * 1000));

	} else {

		while(1) {

			err = recv_task(&manager_rank, task);
			if (err != OK) {
				goto ERROR;
			}

			/* if worker received empty tasks, he should finish his work */
			if (task_is_empty(task)) {
				break;
			}

			do_task(task);

			err = send_task(manager_rank, task);
			if (err != OK) {
				goto ERROR;
			}
		}
	}

	MPI_Finalize();
	return 0;

ERROR:
	print_message(err);
	MPI_Abort(MPI_COMM_WORLD, MPI_ERR_LASTCODE == MPI_SUCCESS? MPI_ERR_UNKNOWN : MPI_ERR_LASTCODE);
	return 1;
}

static inline double
pow(double x, double exp)
{
	double pow = 1.0;
	for (int e = 0; e < exp; e++) {
		pow *= x;
	}
	return pow;
}

static inline double
trust_coef_func(double average_time_on_task_per_group, double average_time_on_task_per_worker)
{
	return pow(average_time_on_task_per_worker / average_time_on_task_per_group, 2); 
}

error_t
calc_trust_coef(const int rank, task_t* task, double *trust_coef)
{
	double new_time_on_task = task->recv_time - task->send_time;

	static work_stat_t* stat_per_worker = NULL;
	static work_stat_t stat_per_group;
	

	if (stat_per_worker == NULL) {

		int size = 0;
		MPI_Comm_size(MPI_COMM_WORLD, &size);

		stat_per_worker = calloc(size, sizeof(work_stat_t));
		if (stat_per_worker == NULL) {
			return ErrOutOfMemory;
		}
	}

	if (!task_is_empty(task) && new_time_on_task > 0) {
		update_work_stat(&stat_per_worker[rank], new_time_on_task);
		update_work_stat(&stat_per_group, new_time_on_task);
	}

	if (stat_per_group.average_time_on_task != 0) {
		*trust_coef = trust_coef_func(stat_per_worker[rank].average_time_on_task, stat_per_group.average_time_on_task);
	} else {
		*trust_coef = 1.0;
	}

	return OK;
}

error_t
new_task(task_t **task)
{
	*task = (task_t *)calloc(1, sizeof(task_t));
	if (*task == NULL) {
		return ErrOutOfMemory;
	}

	return OK;
}

error_t
send_task(const int rank, task_t *task)
{
	int src_rank = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &src_rank);
	
	task->send_time = MPI_Wtime();

	int mpi_err = MPI_Send(task, sizeof(task_t), MPI_CHAR, rank, TASK_TAG, MPI_COMM_WORLD);
	if (mpi_err != MPI_SUCCESS) {
		return ErrMpiSend;
	}
}

error_t
recv_task(int *rank, task_t *task)
{
	int dst_rank = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &dst_rank);
	
	MPI_Status status;
	int mpi_err = MPI_Recv(task, sizeof(task_t), MPI_CHAR, MPI_ANY_SOURCE, TASK_TAG, MPI_COMM_WORLD, &status);
	if (mpi_err != MPI_SUCCESS) {
		return ErrMpiRecv;
	}

	task->recv_time = MPI_Wtime();
	*rank = status.MPI_SOURCE;

	return OK;
}

error_t
give_task(digits_pool_t *a_digits, digits_pool_t *b_digits, const int worker_rank, task_t *task)
{
	static int order_gen = 0;

	double trust_coef = 1.0;
	
	if (mode == DYNAMIC_MODE) {
		error_t err = calc_trust_coef(worker_rank, task, &trust_coef);
		if (err != OK) {
			return err;
		}
	}

	task->result.order = order_gen++;

	int n_digits = (int)(trust_coef * DIGITS_PER_TASK);
	n_digits = (n_digits > MAX_DIGITS_PER_TASK)? MAX_DIGITS_PER_TASK : n_digits;

	task->result.n_digits = pop_digits(a_digits, task->result.a_digits, n_digits);
	task->result.n_digits = pop_digits(b_digits, task->result.b_digits, n_digits);

	return OK;
}

error_t
collect_task_result(task_result_t **results, task_t *task)
{
	task_result_t *new = (task_result_t *)calloc(1, sizeof(task_result_t));
	if (new == NULL) {
		return ErrOutOfMemory;
	}

	memcpy(new, &task->result, sizeof(task_result_t));

	if (*results == NULL || (*results)->order > new->order) {
		new->next = *results;
		*results = new;
		return OK;
	}

	task_result_t *prev = *results;

	for (task_result_t *result = *results; result != NULL; result = result->next) {
		if (result->order > new->order) {
			break;
		}
		prev = result;
	}
	
	new->next = prev->next;
	prev->next = new;

	return OK;
}

void
merge_task_results(task_result_t **results, digits_pool_t *sum_digits)
{
	static digit_t transfer_digit = 0;

	int n_merged = 0;

	for (task_result_t* result = *results; result != NULL; result = result->next) {
		switch (transfer_digit) {
		case 0:
			push_digits(sum_digits, result->sum_digits_v0, result->n_digits);
			transfer_digit = result->transfer_digit_v0;
			break;

		case 1:
			push_digits(sum_digits, result->sum_digits_v1, result->n_digits);
			transfer_digit = result->transfer_digit_v1;
			break;
		}
	}

	if (!pool_is_full(sum_digits)) {
		push_digits(sum_digits, &transfer_digit, 1);
	}

	___clear_tasks_results(results);
}

void
do_task(task_t *task)
{
	digit_t transfer_digit = 0;

	for (int n = 0; n < task->result.n_digits; n++) {
		task->result.sum_digits_v0[n] = add_digits(task->result.a_digits[n], task->result.b_digits[n] + transfer_digit, &transfer_digit);
	}

	task->result.transfer_digit_v0 = transfer_digit;

	transfer_digit = 1;
	for (int n = 0; n < task->result.n_digits; n++) {
		task->result.sum_digits_v1[n] = add_digits(task->result.sum_digits_v0[n] + transfer_digit, 0, &transfer_digit);
	}

	task->result.transfer_digit_v1 = transfer_digit;
}

bool
task_is_empty(task_t *task)
{
	return task->result.n_digits == 0;
}

void
___dump_task(const char* prefix, task_t *task)
{
	printf("%s task{\n"
		   "    send_time:          %.3f\n"
		   "    recv_time:          %.3f\n"
		   "    order:                %d\n"
		   "    a_digits:           {%d, %d, %d...}\n"
		   "    b_digits:           {%d, %d, %d...}\n"
		   "    sum_digits_v0:      {%d, %d, %d...}\n"
		   "    sum_digits_v1:      {%d, %d, %d...}\n"
		   "    transfer_digit_v0:  %d\n"
		   "    transfer_digit_v1:  %d\n"
		   "    n_digits:           %d\n"
	       "}\n",
	       prefix,
	       task->send_time,
	       task->recv_time,
	       task->result.order,
	       task->result.a_digits[0], task->result.a_digits[1], task->result.a_digits[2],
	       task->result.b_digits[0], task->result.b_digits[1], task->result.b_digits[2],
	       task->result.sum_digits_v0[0], task->result.sum_digits_v0[1], task->result.sum_digits_v0[2],
	       task->result.sum_digits_v1[0], task->result.sum_digits_v1[1], task->result.sum_digits_v1[2],
	       task->result.transfer_digit_v0,
	       task->result.transfer_digit_v1,
	       task->result.n_digits);
}

void
___dump_task_results(const char *prefix, task_result_t *results)
{
	char dump[1024];

	sprintf(dump, "%s:\n", prefix);

	for (task_result_t *result = results; result != NULL; result = result->next) {
		sprintf(dump, "%s{ %d } --> ", dump, result->order);
	}

	sprintf(dump, "%s{ nil }\n", dump);

	printf(dump);
}

void
___clear_tasks_results(task_result_t **results)
{
	task_result_t *prev = NULL;

	for (task_result_t *result = *results; result != NULL; result = result->next) {
		if (prev != NULL) {
			free(prev);
		}
		prev = result;
	}

	if (prev != NULL) {
		free(prev);
	}
	*results = NULL;
}

digit_t
add_digits(digit_t a_digit, digit_t b_digit, digit_t *transfer_digit)
{
	digit_t sum_digit = a_digit + b_digit;
	*transfer_digit = sum_digit > MAX_DIGIT_VALUE? 1 : 0;

	return sum_digit % MAX_DIGIT_VALUE;
}

bool
digit_to_str(char *str, digit_t digit)
{
	bool ok = true;
	
	sprintf(str, digit >= 10? "%d" : "0%d", digit);
	
	return ok;
}

bool
str_to_digit(digit_t *digit, char *str)
{
	bool ok = true;
	char *err;
	
	*digit = strtol(str, &err, 10);
	ok = err != NULL || *digit > MAX_DIGIT_VALUE;

	return ok;
}

error_t
new_digits_pool(digits_pool_t **pool)
{
	*pool = (digits_pool_t *)calloc(1, sizeof(digits_pool_t));
	if (*pool == NULL) {
		return ErrOutOfMemory;
	}

	return OK;
}

error_t
read_digits(int fd, digits_pool_t *pool, int *n_digits)
{
	___clear_digits(pool);

	bool ok = true;
	char str[CHARS_PER_DIGIT + 1];

	int size_buf = sizeof(str) * DIGITS_PER_POOL;

	char* buf = (char*)calloc(size_buf, sizeof(char));
	if (buf == NULL) {
		return ErrOutOfMemory;
	}

	int n_read_chars = read(fd, buf, size_buf);
	if (n_read_chars < 0) {
		return ErrUnistdRead;
	}


	*n_digits = n_read_chars / sizeof(str);

	digit_t digit = 0;

	for (int n = 0; n < *n_digits; n++)
	{	
		memcpy(str, buf + n * sizeof(str), sizeof(str));
		if (str[sizeof(str)-1] != ' ') {
			return ErrFileFormat;
		}
		str[sizeof(str)-1] = '\0';

		ok = str_to_digit(&digit, str);
		if (!ok) {
			return ErrFileFormat;
		}
		push_digits(pool, &digit, 1);
	}

	return OK;
}

error_t
write_digits(int fd, digits_pool_t *pool)
{
	int n_digits = 0;

	bool ok = true;
	char str[CHARS_PER_DIGIT + 1];

	int size_buf = sizeof(str) * DIGITS_PER_POOL;

	char* buf = (char*)calloc(size_buf, sizeof(char));
	if (buf == NULL) {
		return ErrOutOfMemory;
	}

	digit_t digit = 0;
	int n = 0;

	for (n = 0; !pool_is_empty(pool); n++)
	{
		pop_digits(pool, &digit, 1);

		ok = digit_to_str(str, digit);
		if (!ok) {
			return ErrInternal;
		}

		str[sizeof(str)-1] = ' ';

		memcpy(buf + n * sizeof(str), str, sizeof(str));
	}


	n_digits = n;

	int n_written_chars = write(fd, buf, n_digits * sizeof(str));
	if (n_written_chars < n_digits * sizeof(str)) {
		return ErrUnistdWrite;
	}

	___clear_digits(pool);

	return OK;
}

int
pop_digits(digits_pool_t *pool, digit_t* digits, int n_digits)
{
	int n = 0;

	for (n = 0; n < n_digits && pool->n_popped_digits < pool->n_pushed_digits; n++) {
		digits[n] = pool->digits[pool->n_popped_digits++];
	}

	return n;
}

int
push_digits(digits_pool_t *pool, digit_t* digits, int n_digits)
{
	int n = 0;

	for(n = 0; n < n_digits && pool->n_pushed_digits < DIGITS_PER_POOL; n++) {
		pool->digits[pool->n_pushed_digits++] = digits[n];
	}

	return n;
}

bool
pool_is_full(digits_pool_t *pool)
{
	return pool->n_pushed_digits == DIGITS_PER_POOL;
}

bool
pool_is_empty(digits_pool_t *pool)
{
	return pool->n_popped_digits == pool->n_pushed_digits;
}

void
___clear_digits(digits_pool_t *pool)
{
	pool->n_popped_digits = pool->n_pushed_digits = 0;
}

void
___dump_digits(const char *prefix, digits_pool_t *pool)
{
	printf("%s digits_pool{\n"
		   "    .digits = {%d, %d, %d...}\n"
		   "    .n_pushed_digits = %d\n"
		   "    .n_popped_digits = %d\n"
	       "}\n",
	       prefix,
	       pool->digits[0], pool->digits[1], pool->digits[2],
	       pool->n_pushed_digits,
	       pool->n_popped_digits);
}


void
print_message(error_t err)
{
	char msg[MAX_MESSAGE_SIZE];

	switch (err / ERROR_TYPE) {
	case ErrInternal:
		internal_message(msg, err);
		break;

	case ErrErrno:
		errno_message(msg, err);
		break;

	case ErrMpi:
		mpi_message(msg, err);
		break;

	default:
		sprintf(msg, "Unknown error");
		break;
	}

	fprintf(stderr, "Error: %s\n", msg);
	fclose(stderr);
}

void
internal_message(char *msg, error_t err)
{
	switch (err) {
	case ErrOutOfMemory:
		sprintf(msg, "Out of memory");
		break;

	case ErrFileFormat:
		sprintf(msg, "Invalid file format");
		break;

	case ErrDiffNumLen:
		sprintf(msg, "Different numbers length");
		break;

	default:
		sprintf(msg, "Unknown error");
		return;
	}
}

void
errno_message(char *msg, error_t err)
{
	switch (err) {
	case ErrUnistdOpen:
		sprintf(msg, "on \"open\": %%s");
		break;

	case ErrUnistdWrite:
		sprintf(msg, "on \"write\": %%s");
		break;

	case ErrUnistdRead:
		sprintf(msg, "on \"read\": %%s");
		break;

	default:
		sprintf(msg, "Unknown error");
		return;
	}

	sprintf(msg, msg, strerror(errno));
}

void
mpi_message(char *msg, error_t err)
{
	switch (err) {
	case ErrMpiRecv:
		sprintf(msg, "on \"MPI_Recv\": %%s");
		break;

	case ErrMpiSend:
		sprintf(msg, "on \"MPI_Send\": %%s");
		break;

	default:
		sprintf(msg, "Unknown error");
		return;
	}

	int len = 0;
	char mpi_msg[MAX_MESSAGE_SIZE];

	int mpi_err = MPI_Error_string(MPI_ERR_LASTCODE, mpi_msg, &len);
	if (mpi_err != MPI_SUCCESS) {
		strcpy(mpi_msg, "unknown error");
	}
	
	sprintf(msg, msg, mpi_msg);
}

void
update_work_stat(work_stat_t* stat, double new_time_on_task)
{
	stat->average_time_on_task = (stat->average_time_on_task * stat->n_tasks + new_time_on_task) / (stat->n_tasks + 1);
	stat->n_tasks++;
}

void
help() {
	printf("Flags:\n"
		   "	-s, --static - to run in static mode (default is dynamic),\n"
	       "	-h, --help   - to see this note.\n");
}