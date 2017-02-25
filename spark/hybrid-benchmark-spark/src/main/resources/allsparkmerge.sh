#!/bin/bash


for dir in $(find results/spark/ -maxdepth 3 -mindepth 3 -type d);do
	#echo $dir
	if [ -f /$dir/newsparkmerge.sh ];then
		rm -f $dir/newsparkmerge.sh
	fi
	cp newsparkmerge.sh $dir/newsparkmerge.sh
	cd $dir
	./newsparkmerge.sh $dir
	cd ../../../../../
	rm $dir/newsparkmerge.sh
done


