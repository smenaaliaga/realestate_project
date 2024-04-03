# run.sh
#!/bin/bash
source /home/choribread/environments/realestate_scraper/bin/activate
/home/choribread/environments/realestate_scraper/bin/python3 /home/choribread/realestate_scraper/main.py > /home/choribread/realestate_scraper/log/cron.log 2>&1
