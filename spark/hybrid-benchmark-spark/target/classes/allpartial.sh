#!/bin/bash


for dir in $(find results/spark/ -maxdepth 3 -mindepth 3 -type d);do
	#echo $dir
	if [ -f /$dir/partialsparkmerge.sh ];then
		rm -f $dir/partialsparkmerge.sh
	fi
	cp partialsparkmerge.sh $dir/partialsparkmerge.sh
	cd $dir
	./partialsparkmerge.sh $dir
	cd ../../../../../
	rm $dir/partialsparkmerge.sh
done


