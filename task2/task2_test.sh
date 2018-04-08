#!/bin/bash

tests="task2 task2_2"

for test in $tests
do
	echo "hello"
	if mpicc $test.c -o $test ; then
		for (( N = 4; N <= 4; N += 4 ))
		do
			echo "=== RUN  Test2 for $test with CommSize = $N"
			sudo mpirun -n $N ./$test
			echo "=== PASS Test2 for $test with CommSize = $N"
		done
	else
		echo "Error: couldn't compile $test."
		exit 1
	fi
	echo
	rm -f $test
done