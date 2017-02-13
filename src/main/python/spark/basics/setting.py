import os
import sys

"""
1. Go to File > Project Structure, select SDK, selcet the Python3
2. Select 'use specified interpreter' and select the SDK you just installed for steps 1
"""

# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/opt/spark210"

# Need to Explicitly point to python3 if you are using Python 3.x
os.environ['PYSPARK_PYTHON']="/usr/local/Frameworks/Python.framework/Versions/3.5/bin/python3"

#You might need to enter your local IP
#os.environ['SPARK_LOCAL_IP']="192.168.2.138"

#Path for pyspark and py4j
sys.path.append("/usr/local/opt/spark210/python")
sys.path.append("/usr/local/opt/spark210/python/lib/py4j-0.10.4-src.zip")


APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

SRC = os.path.join(APP_DIR+'/src')
MAIN = os.path.join(SRC+'/main')
SPARK_WAREHOUSE = os.path.join(MAIN+'/python/spark-warehouse')
RESOURCES = os.path.join(MAIN+'/resources')
GENERAL_DATA = os.path.join(RESOURCES+'/general')
BBALL_STAT = os.path.join(RESOURCES+'/bballStat')
MOVIE_DATA = os.path.join(RESOURCES+'/moviedata')
SALES_DATA = os.path.join(RESOURCES+'/sales')
HIVE_FILES = os.path.join(RESOURCES+'/hivefiles')


sys.path.insert(0,APP_DIR)
sys.path.insert(1,RESOURCES)
sys.path.insert(2,GENERAL_DATA)
sys.path.insert(3,BBALL_STAT)
sys.path.insert(4,MOVIE_DATA)
sys.path.insert(5,SALES_DATA)
sys.path.insert(6,HIVE_FILES)
sys.path.insert(7,SPARK_WAREHOUSE)