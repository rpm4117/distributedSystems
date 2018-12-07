import sys

id = sys.argv[1].split(",")[0]
date = sys.argv[1].split(",")[1]
content = sys.argv[1].split(",")[2]
# get only twitters posted in 2017
if "2017" in date:
    print(content)
