#!/bin/bash

tests="task3"

for test in $tests
do
	if mpicc $test.c -o $test ; then
		for (( N = 4; N <= 4; N += 4 ))
		do
			echo "=== RUN  Test2 for $test with CommSize = $N"
			mpirun -n $N ./$test
			echo "=== PASS Test2 for $test with CommSize = $N"
			echo "=== RUN  Test2 for $test with CommSize = $N (static)"
			mpirun -n $N ./$test --static
			echo "=== PASS Test2 for $test with CommSize = $N"
			echo
		done
	else
		echo "Error: couldn't compile $test."
		exit 1
	fi
	rm -f $test
done