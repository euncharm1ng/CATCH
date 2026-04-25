#!/bin/bash

mkdir -p contracts parsed raw
# python3 fetch.py --full-tx --logs 2585770 --end 2585772
# python3 parse.py --all --db
python trace.py 0xEA57D4a208e9F24150F404928357172Daf6fA60a
python trace.py 0xEA57D4a208e9F24150F404928357172Daf6fA60a --mode sandwich
python trace.py 0x075a531ea2ba4cAadda043358747FAbcC588851a
python trace.py 0x075a531ea2ba4cAadda043358747FAbcC588851a --mode arb
python trace.py 0x5A89D0400AB44bf82dC39f54eD4943D40906eC5D
python trace.py 0x5A89D0400AB44bf82dC39f54eD4943D40906eC5D --mode sandwich
python trace.py 0x999b9117D378434eA4B4B1042bc056B7dd81D215
python trace.py 0x999b9117D378434eA4B4B1042bc056B7dd81D215 --mode sandwich
python trace.py 0xDDe99AE4A2177e9a5C99D14AA9Baae5706A42a6D
python trace.py 0xDDe99AE4A2177e9a5C99D14AA9Baae5706A42a6D --mode sandwich