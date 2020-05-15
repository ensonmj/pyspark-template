#!/usr/bin/python
#-*- coding:gbk -*-
#/***************************************************************************
# *
# * Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# *
# **************************************************************************/
#/**
# **/

import sys
#sign module
import sign

def main():
    for line in sys.stdin:
        line = line.strip()
        if line == "":
            continue
        cuid = str(sign.creat_sign_fs64(line))
        print cuid

main()  
