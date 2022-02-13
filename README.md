# Code-Challenge



# BUILD
Under /app, in file db, add secret key and access id. 
docker build -t app .
docker run -d -p 8080:8080 app   

Or
cd app
export Flask_run=api.py
python -m flask run 

# POSTMAN COLLECTION LINK
https://www.getpostman.com/collections/a310a1c4d0cd63dbfe71

# Incompetled Part:
Unit Testing for apis.

