
#!/bin/bash

recordSize=$1
totalFileSize=$2
num_threads=(1 2 4 8 12 24 48)

for num_thread in ${num_threads[@]}
do
    echo "sequential write test"
    ./hw -t $num_thread -r $recordSize -s $totalFileSize -i 0
    echo "sequential read test"
    ./hw -t $num_thread -r $recordSize -s $totalFileSize -i 1
    echo "random write test"
    ./hw -t $num_thread -r $recordSize -s $totalFileSize -i 2
    echo "random read test"
    ./hw -t $num_thread -r $recordSize -s $totalFileSize -i 3 
done




