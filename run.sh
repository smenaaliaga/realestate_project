# run.sh
#!/bin/bash
cd /home/choribread/realestate_scraper/
source ../environments/realestate_scraper/bin/activate
python main.py > log/cron.log 2>&1
