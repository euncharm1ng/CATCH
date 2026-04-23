#!/bin/bash

mkdir -p contracts parsed raw
# python3 fetch.py --full-tx --logs 2585770 --end 2585772
python3 parse.py --all --db
python3 trace.py 0xEA57D4a208e9F24150F404928357172Daf6fA60a --mode sandwich
