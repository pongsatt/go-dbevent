docker run --name mysql -p 3306:3306 \
-e MYSQL_DATABASE=testdb \
-e MYSQL_ROOT_PASSWORD=my-secret-pw \
mysql