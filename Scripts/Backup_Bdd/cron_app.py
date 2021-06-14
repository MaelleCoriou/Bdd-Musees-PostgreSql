#!/usr/bin/env python
from crontab import CronTab
import backup

# Creating an object from the class
## Using the root user
cron = CronTab(user="root")
 
## Using the current user
my_cron = CronTab(user=True)
 
# Creating an object from the class into a file
file_cron = CronTab(tabfile="filename.tab")

job = cron.new(command='python backup.py')

job.hour.every(24)
job.hour.also.on(16)

cron.write()

print(job.enable())
print(job.enable(False))
print(job.is_valid())