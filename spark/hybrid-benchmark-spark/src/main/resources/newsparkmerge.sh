#!/bin/bash
#turns all file* folders of spark result into one result csv 

num=0
numparts=0
#name=da
bool=true
for i in $(ls); do
	if [[ $i =~ ^fil.* ]];
	then
		bool=false
		txt=`cat $i/part-00000`
		txt2=`cat $i/part-00001`
		txt3=`cat $i/part-00002`
		if [[ -z $txt ]];
		then
		    tdad=3
        else

            files[$num]=`cat $i/part-00000`
		    num=$((num+1))
		    numparts=$((numparts+1))
            declare name=$i
        fi
        if [[ -z $txt2 ]];
		then
		    tdad=3
        else
            files[$num]=`cat $i/part-00001`
		    num=$((num+1))
            declare name=$i
        fi
        if [[ -z $txt3 ]];
		then
		    tdad=3
        else
            files[$num]=`cat $i/part-00002`
		    num=$((num+1))
            declare name=$i
        fi
		#echo $i
#name=${name
		
	fi
done
if $bool;
then
#	echo no results found [directories starting with file]
	exit
fi



pattern='(,,'
numlines=$(grep -o '(,,' <<< "${files[@]}" | wc -l)
th=$((numlines/numparts/2))
filename=$name'+'$th'+'

printf "%s\n" "${files[@]}" > "${filename##*_}".csv

find -type d -name "fil*" -exec rm -rf {} +;

