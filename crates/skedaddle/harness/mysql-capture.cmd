@echo off
python -I -X "pycache_prefix=%M1_OUTPUT%/pycache" "%~dp0mysql-capture.py" %*
