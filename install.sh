if ! command -v python3 &> /dev/null; then echo "python3 missing"; exit 1; fi
if ! command -v pip3 &> /dev/null; then echo "pip3 missing"; exit 1; fi
python3 -m pip install --upgrade pip > /dev/null

# install dependencies with pipreqs (requirements.txt)
pip3 install black > /dev/null
pip3 install pipreqs > /dev/null
rm -rf requirements.txt > /dev/null
pipreqs . > /dev/null
pip3 install -r requirements.txt > /dev/null

# debug: pip fails on metadata
# find /opt/homebrew/lib/python3.11/site-packages -empty -type d -delete

# run link scraper
python3 src/scrape_links.py
