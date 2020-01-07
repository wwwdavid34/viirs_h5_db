#!/usr/bin/env python3

from import_to_db import ImportToDB
import sys

file=sys.argv[1]
b=ImportToDB(file, server='eogdev')
