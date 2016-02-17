#!/bin/bash

uwsgi --socket 0.0.0.0:8080 --master -p 2 --locks 1 --wsgi-file app.py --callable app &
nginx -g "daemon off;";