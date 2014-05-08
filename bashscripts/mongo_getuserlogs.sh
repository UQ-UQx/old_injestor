#!/bin/bash
if [ $# -ne 1 ]
then
  echo "Usage: getuserclicks USER_ID"
  exit 65
fi
mongo 127.0.0.1/logs --eval "var c = db.clickstream.find({'context.user_id':$1}); while(c.hasNext()) {printjson(c.next())}" >> $1.json
