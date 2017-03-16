#!/bin/bash


for dir in $(find results/spark/ -maxdepth 3 -mindepth 3 -type d);do

	if [ -f /$dir/csvreduce.sh ];then
		rm -f $dir/csvreduce.sh
	fi
	echo $dir
	cp csvreduce.sh $dir/csvreduce.sh
	cd $dir
	./csvreduce.sh $dir
	cd ../../../../../
	#rm $dir/csvreduce.sh
done


