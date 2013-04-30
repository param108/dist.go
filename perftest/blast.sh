#! /bin/bash
if [ "$GOPATH"a = "a" ]
then
	echo "NEED TO SETUP GOPATH"
fi

for ((i=0;i<$1;i++))
do
	$GOPATH/bin/distserver 2>&1 > /dev/null &
done

for ((i=0;i<$1;i++))
do
wait
done
