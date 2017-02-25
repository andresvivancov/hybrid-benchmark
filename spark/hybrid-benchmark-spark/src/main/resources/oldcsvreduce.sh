#!/bin/bash
#reduces all file* folders of spark result csv into a one line csv
path=$1
win=$(echo $path | cut -f3 -d/)
batch=$(echo $path | cut -f4 -d/)
work=$(echo $path | cut -f5 -d/)

#IFS=$'/' parts=(${path})
#echo $1
num=0
numparts=0
#name=da
bool=true
for i in $(ls); do
	if [[ $i == *csv ]];
	then
	if [[ $i == new* ]];
	then
	    dd=3
	else
		txt=`cat $i`
		if [[ -z $txt ]];
		then
		    dd=4
        else
            num=0
            sumlat=0
            while read -r line
            do
                #echo $line
                #parts=$(echo ${line} | tr "," "\n")
                IFS=$',' lines=(${line})
                num=$((num+1))
                sumlat=$((sumlat+lines[13]))
            done <<< "$txt"
            lat=$((sumlat/num))
            IFS=$'+' th=(${i})
            tuple="spark,$win,$batch,$work,$lat,$((th[1])),$num"
            #echo ${tuple}
            printf "%s\n" "${tuple}" > "new-${th##*_}".csv
        fi
        fi
		#echo $i
#name=${name
		
	fi
done

#find . -type f ! -name "new*.csv" -delete