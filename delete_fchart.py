import pymysql
from datetime import datetime, timedelta
import requests
import json
from apscheduler.schedulers.blocking import BlockingScheduler
from data_config import *

def exec_cron():

    db = pymysql.connect(
        host=host,
        user=user,
        password=password,
        charset='utf8mb4',
        database=database
    )

    now = datetime.now()
    fd = now.strftime('%Y-%m-%d %H:%M:%S')
    curs = db.cursor()

    isql = """delete from fchart where rods = %s and dt < %s"""

    m5_now = now - timedelta(days = 7)
    m5_fd = m5_now.strftime('%Y-%m-%d %H:%M:%S')
    curs.execute(isql, ('min5', m5_fd))

    m15_now = now - timedelta(days = 21)
    m15_fd = m15_now.strftime('%Y-%m-%d %H:%M:%S')
    curs.execute(isql, ('min15', m15_fd))

    m30_now = now - timedelta(days = 42)
    m30_fd = m30_now.strftime('%Y-%m-%d %H:%M:%S')
    curs.execute(isql, ('min30', m30_fd))

    h1_now = now - timedelta(days = 84)
    h1_fd = h1_now.strftime('%Y-%m-%d %H:%M:%S')
    curs.execute(isql, ('hour1', h1_fd))

    h4_now = now - timedelta(days = 336)
    h4_fd = h4_now.strftime('%Y-%m-%d %H:%M:%S')
    curs.execute(isql, ('hour4', h4_fd)) 

    d1_now = now - timedelta(days = 2016)
    d1_fd = d1_now.strftime('%Y-%m-%d %H:%M:%S')
    curs.execute(isql, ('day1', d1_fd))
    db.commit()   

sched = BlockingScheduler()
sched.add_job(exec_cron, 'cron', hour='*/4')

sched.start()
