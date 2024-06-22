python3 -m pip install --upgrade pip

# find out dependencies
rm -rf requirements.txt
pip install pipreqs
pipreqs .

# install dependencies
pip install black
pip install -r requirements.txt
