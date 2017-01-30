#!/bin/bash


for dir in $(find results/spark/ -maxdepth 3 -mindepth 3 -type d);do
	#echo $dir
	if [ -f /$dir/sparkmerge.sh ];then
		rm -f $dir/sparkmerge.sh
	fi
	cp sparkmerge.sh $dir/sparkmerge.sh
	cd $dir
	./sparkmerge.sh
	cd ../../../../../
	rm $dir/sparkmerge.sh
done


