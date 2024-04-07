# scrape all links
python3.11 ./src/scrape_links.py

# get list of all ./data/*csv files
csvFiles=$(ls ./data/*csv)
printf "csvFiles: $csvFiles\n"
