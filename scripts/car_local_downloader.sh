CARLOCAL="carLocalInput.zip"
wget -O ../input/${CARLOCAL} "https://www.dropbox.com/scl/fi/l4bfcch2fnm8rd3n10w6n/7s9ewtys69aik5p8bwapbazjs2u9l8vv.zip?rlkey=khx05l4ywd2uyzyjsf7l9j0mw&dl=0"
unzip -q ../input/${CARLOCAL} -d ../input/
rm ../input/${CARLOCAL}
