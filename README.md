## to restart kite streaming
cd /home/ubuntu/market_analysis
ps aux | grep kite_web_socket_streaming.py
kill the process
python3 kite_util.py
# open the link in browser in incognito mode and login
# copy the value of request_token (example: request_token=gFmPkxWKt5vVgx4xCtOZSpHzii5GAt3f) and paste in the terminal
#Ensure that output says something like (here is your access token: antCFLCOr8wU1B78d749IX3kYdXTaLmq)


nohup python3 kite_web_socket_streaming.py &> /tmp/kitestream.log &
#data will be logged in /data/kite_websocket_data directory

ZR8231
Abhimonu@236



## to update upcoming result dates
cd /home/ubuntu/market_analysis/text_files
# truncate the result date file
> result_dates.txt

vim result_dates.txt
paste all the entries as it is (bina smartness dikhaye)
exit vim
python3 update_nse_result_dates.py

ensure output says "parsing" as many times as their are entries in the file


## to check if the event notification is working
script name is: bse_announcements.py
`ps aux | grep bse_announcements.py` should return a valid python process

to stop this process, execute kill PID (PID= process id from ps aux command)
to run this process, execute `nohup python3 bse_announcements.py &> nohup.out &` from market_analysis directory


#to get bse result announcements manually, execute following and do whatever script asks
python3 get_historical_bse_announcements.py
