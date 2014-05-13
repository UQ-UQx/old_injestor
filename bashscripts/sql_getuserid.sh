#!/bin/bash
if [ $# -ne 2 ]
then
  echo "Usage: sql_getuserid SQLPASSWORD USER_ID"
  exit 65
fi
mysql -u root -p$1 -e "SELECT id from UQx_Think101x_1T2014.auth_user WHERE username = '$2'"