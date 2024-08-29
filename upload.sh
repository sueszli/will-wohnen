# read user argument
file_path=$1
if [ -z $file_path ]; then echo "no argument given - provide file path"; exit 1; fi
if [ ! -f $file_path ]; then echo "file not found"; exit 1; fi
if [ ! -s $file_path ]; then echo "file is empty"; exit 1; fi
echo "file found: $file_path"

# check .gitignore
if [ ! -f ".gitignore" ]; then echo ".gitignore file does not exist."; exit 1; fi
if ! grep -q "tmp\|data" .gitignore; then echo ".gitignore file does not contain 'tmp' or 'data'."; exit 1; fi
echo "validated .gitignore"

# create tmp directory
rm -rf tmp
mkdir tmp
echo "created tmp directory"

# copy file to tmp directory
cp $file_path tmp
echo "copied $file_path to tmp directory"

# create checksum file
checksum=$(md5sum tmp/$(basename $file_path) | awk '{ print $1 }')
echo $checksum > tmp/$(basename $file_path).md5
echo "created checksum file: $(basename $file_path).md5"

# split file into chunks in tmp directory
chunk_size=$((50 * 1024 * 1024))
split -b $chunk_size tmp/$(basename $file_path) tmp/$(basename $file_path)-chunk-

# create data directory
rm -rf data
mkdir data
echo "created data directory"

# copy checksum
mv tmp/$(basename $file_path).md5 data

# iterate over chunks, push to git
num_chunks=$(ls tmp/$(basename $file_path)-chunk-* | wc -l)
counter=0
for chunk in tmp/$(basename $file_path)-chunk-*; do
    counter=$((counter + 1))
    progress_str=$(printf "%d/%d" $counter $num_chunks)
    
    mv $chunk data
    git add .
    git commit -m "auto commit: $(basename $chunk) $progress_str"
    git push

    echo "🟢 pushed $(basename $chunk) $progress_str"
done

# delete tmp directory
rm -rf tmp

echo "🟢 done"
exit 0