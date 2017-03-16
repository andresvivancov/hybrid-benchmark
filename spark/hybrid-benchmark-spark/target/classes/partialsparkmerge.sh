#!/bin/bash
#turns all file* folders of spark result into one result csv 

path=$1
win=$(echo $path | cut -f3 -d/)
batch=$(echo $path | cut -f4 -d/)
work=$(echo $path | cut -f5 -d/)


numparts=0
num=0
#name=da
bool=true
for i in $(ls); do
	if [[ $i =~ ^fil.* ]];
	then
		bool=false
		txt=`cat $i/part-00000`
		txt2=`cat $i/part-00001`
		txt3=`cat $i/part-00002`
		sumlat=0
		num=0
		if [[ -z $txt ]];
		then
		    tdad=3
        	else
			bool=false
		    numparts=$((numparts+1))    
		    
		    
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
			
			lat=$((sumlat/num))
			th=$((num/2))
			#IFS=$'+' th=(${i})
			tuple="spark,$win,$batch,$work,$lat,$th,$num"
			files[$numparts]=$tuple
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

#printf "%s\n" "${files[@]}" > "$(date +%s%3N)".csv
printf "%s\n" "${files[@]}" > "${name##*_}".csv

find -type d -name "fil*" -exec rm -rf {} +;






           
           
         
