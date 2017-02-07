import os
import sys

"""
1. Go to File > Project Structure, select SDK, selcet the Python3
2. Select 'use specified interpreter' and select the SDK you just installed for steps 1
"""

# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/spark210"

# Need to Explicitly point to python3 if you are using Python 3.x
os.environ['PYSPARK_PYTHON']="/Library/Frameworks/Python.framework/Versions/3.5/bin/python3"

#You might need to enter your local IP
#os.environ['SPARK_LOCAL_IP']="192.168.2.138"

#Path for pyspark and py4j
sys.path.append("/usr/local/Cellar/apache-spark/spark210/python")
sys.path.append("/usr/local/Cellar/apache-spark/spark210/python/lib/py4j-0.10.4-src.zip")
