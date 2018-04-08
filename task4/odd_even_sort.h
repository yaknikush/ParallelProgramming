#ifndef __ODD_EVEN_SORT_H__
#define __ODD_EVEN_SORT_H__

#include <string.h>

typedef int (*compare_func)(const void *, const void *);

#define COMPARE_PROTOTYPE(T)               \
int                                        \
compare_##T(const void *x, const void* y);

COMPARE_PROTOTYPE(char)
COMPARE_PROTOTYPE(short)
COMPARE_PROTOTYPE(int)
COMPARE_PROTOTYPE(long)
COMPARE_PROTOTYPE(double)
COMPARE_PROTOTYPE(float)
#undef COMPARE_PROTOTYPE

#define has_compare_of(T) (       \
	strcmp(#T, "char")   == 0 ||  \
    strcmp(#T, "short")  == 0 ||  \
    strcmp(#T, "int")    == 0 ||  \
    strcmp(#T, "long")   == 0 ||  \
    strcmp(#T, "double") == 0 ||  \
    strcmp(#T, "float")  == 0     \
)


void
odd_even_sort(void *elems, int n_elems, int elem_size, compare_func compare_elems);

#endif