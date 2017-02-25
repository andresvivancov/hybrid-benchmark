#!/bin/bash
#turns all file* folders of spark result into one result csv 
path=$1
win=$(echo $path | cut -f3 -d/)
batch=$(echo $path | cut -f4 -d/)
work=$(echo $path | cut -f5 -d/)

numparts=0
#name=da
bool=true
num=0
sumlat=0

for i in $(ls); do
	if [[ $i =~ ^fil.* ]];
	then
	    if  $bool ;
	    then
	        bool=false
	    else
            #bool=false
            txt=`cat $i/part-00000`
            txt2=`cat $i/part-00001`
            txt3=`cat $i/part-00002`
            if [[ -z $txt ]];
		    then
		        da=3
		    else
                numparts=$((numparts+1))
            fi
            declare name=$i
            while read -r line
            do
                 IFS=$',' lines=(${line})
                 num=$((num+1))
                 sumlat=$((sumlat+lines[13]))
            done <<< "$txt"
            while read -r line
            do
                 IFS=$',' lines=(${line})
                 num=$((num+1))
                 sumlat=$((sumlat+lines[13]))
            done <<< "$txt2"
            while read -r line
            do
                 IFS=$',' lines=(${line})
                 num=$((num+1))
                 sumlat=$((sumlat+lines[13]))
            done <<< "$txt3"
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

lat=$((sumlat/num))
th=$((num/numparts/2))
#IFS=$'+' th=(${i})
tuple="spark,$win,$batch,$work,$lat,$th,$num"
echo ${tuple}
#printf "%s\n" "${tuple}" > "new-${th##*_}".csv


#pattern='(,,'
#numlines=$(grep -o '(,,' <<< "${files[@]}" | wc -l)

#filename=$name'+'$th'+'

printf "%s\n" "${tuple}" > "new-${name##*_}".csv

find -type d -name "fil*" -exec rm -rf {} +;

