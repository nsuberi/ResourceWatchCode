#!/bin/sh

# setup crontab with current environment
touch cron.log
printenv > cron.tmp
echo "$(cat time.cron) cd $(pwd) && python main.py >> cron.log 2>&1" >> cron.tmp
crontab cron.tmp
rm cron.tmp

# run cron and print logs
cron && tail -f cron.log