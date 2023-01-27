#!/bin/env python
#Scripts reads the ibm.csv file and pushes one rows per second

#import pandas as pd
from time import sleep
#from json import dumps
import sys

#get_df = pd.read_csv('ibm.csv')

with open('ibm.csv','r',encoding='utf-8') as ibm:
    while True:
        line = ibm.readline()
        sys.stdout.write(str(line))
        sleep(1)
