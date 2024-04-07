# scrape all links
python3.11 ./src/scrape_links.py

# commit and push
git add . && git commit -m "up" && git push

# get last scrape
last_links=$(ls -t ./data/*csv | head -n 1)
printf "lastest link scrape: $last_links\n"

# open all in browser
counter=0
total=$(wc -l < $last_links)
while IFS= read -r line; do
    progress=$((counter*100/total))
    printf "progress: $progress%%\n"

    open -a "Google Chrome" $line

    counter=$((counter+1))
done < "$last_links"
