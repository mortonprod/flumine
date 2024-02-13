####Process historical data####
DIR=/home/mortonprod/data

# mkdir -p $DIR/output
# #Untar all files folder
# for tar in $DIR/*.tar;
# do
#     tar -xf $tar -C $DIR/output
#     mv $tar "$tar-processed"
# done

# find $DIR/output -exec bunzip2 {} \;

for fileFull in `find $DIR/output -type f`;
do
    path=`dirname $fileFull`
    file=`basename $fileFull`
    rootDir=`basename $path`
    if [[ $rootDir == $file ]]
    then
        eventDir="${path/output/"events"}" 
        mkdir -p $eventDir
        mv $fileFull $eventDir
    else
        marketDir="${path/output/"market"}" 
        mkdir -p $marketDir
        mv $fileFull $marketDir
    fi
done