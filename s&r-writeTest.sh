
#!/bin/bash

recordSize=$1
totalFileSize=$2
num_threads=(1 2 4 8 12 24 48)
# sequetial read test
echo "sequential write test"
for num_thread in ${num_threads[@]}
do
    ./hw -t $num_thread -r $recordSize -s $totalFileSize -i 0
done


# random read test
echo "random write test"
for num_thread in ${num_threads[@]}
do
     #echo $filename
    ./hw -t $num_thread -r $recordSize -s $totalFileSize -i 2
done

