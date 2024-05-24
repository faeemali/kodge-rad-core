#!/bin/bash

> output
while read line;
do
	echo ${line} >> output
done 
