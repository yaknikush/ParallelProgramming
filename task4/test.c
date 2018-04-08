static int
merge_sort(void *left_buf, int left_count,
           void *right_buf, int right_count, int size, int (*comp)(const void *, const void *))
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

int compare_int(const void *x, const void *y)
{
	int z = *(const int *)x - *(const int *)y;
	return (z > 0) - (z < 0);
}

int main()
{
		int a = {1, 13, 42, 102, 111, 131};
		int b = {0, 1, 2, 3, 48, 49, 89, 110, 119, 221, 221};

		merge_sort(a, sizeof(a) / sizeof(a[0]), b, sizeof(b) / sizeof(b[0]), sizeof(int), compare_int);
}