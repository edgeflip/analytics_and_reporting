cut -d" " -f 1 all-individual-aboutme.txt | sort | uniq > all-user-ids.txt

sort -R all-user-ids.txt > all-user-ids-randomized.txt

if [ ! -z "$1" ]
then
    head -n $1 all-user-ids-randomized.txt > user-ids-randomized-sample.txt
else
    cat all-user-ids-randomized.txt > user-ids-randomized-sample.txt
fi

num_instances=$(cat user-ids-randomized-sample.txt | wc -l)

num_test=`echo "($num_instances+5-1) / 5" | bc`

head -n $num_test user-ids-randomized-sample.txt > test-user-ids.txt

tail -n +$(expr $num_test + 1) user-ids-randomized-sample.txt > train-user-ids.txt

sort test-user-ids.txt > test-user-ids_sorted.txt
sort train-user-ids.txt > train-user-ids_sorted.txt
