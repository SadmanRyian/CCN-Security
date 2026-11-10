#! /bin/sh
#echo "Hint: Remeber to recompile application after modification has been made"
#sh compileApp.sh
java -Xmx1024M -cp .:lib/ECLA.jar:lib/DTNConsoleConnection.jar core.DTNSim $*
