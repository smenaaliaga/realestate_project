# run.sh
#!/bin/bash
cd /home/seldon/realestate_project/
source ../environments/realestate_project/bin/activate
python main.py > log/cron.log 2>&1
