#!/bin/bash

if [[ $1 == "" ]]; then
	echo "Error: invalid number of arguments. Number of processes wasn't specifed."
	echo "The usage of $0 is:"
	echo "$0 [NUM_OF_PROCESSES] [NUM_OF_TESTS]"
	exit 1
fi

if [[ $2 == "" ]]; then
	echo "Error: invalid number of arguments. Number of tests wasn't specified."
	echo "The usage of $0 is:"
	echo "$0 [COMM_SIZE] [NUM_OF_TESTS]"
	exit 1
fi

rm -f got expected
touch got expected

for (( N = 1; N < $1; N++ ))
do
	echo $N >> expected
done

tests="task1 task1_2"

for test in $tests
do
	if mpicc $test.c -o $test ; then
		for (( N = 1; N < $2; N++ ))
		do
			echo " ==== RUN  Test1 #$N for $test with CommSize = $1"

			start=$(date +%s)
			mpirun -n $1 ./$test > got
			elapsed=$(($(date +%s) - $start))

			dif=$(diff -q got expected)

			if [[ $dif == "Файлы got и expected различаются" ]] ; then
				echo " ==== FAIL Test1 #$N"
			else
				echo " ==== PASS Test1 #$N (took $elapsed seconds)"
			fi
		done
	else
		echo "Error: couldn't compile $test."
	fi
	rm -f $test
done

rm -f got expected

exit 0