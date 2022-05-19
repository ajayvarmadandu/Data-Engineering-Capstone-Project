
rm  /home/anabig114249/departments.avsc 
rm /home/anabig114249/dept_emp.avsc 
rm /home/anabig114249/dept_manager.avsc 
rm /home/anabig114249/employees.avsc 
rm /home/anabig114249/salaries.avsc 
rm /home/anabig114249/titles.avsc 


hdfs dfs -rm -r /user/anabig114249/hive/warehouse/departments
hdfs dfs -rm -r /user/anabig114249/hive/warehouse/dept_emp
hdfs dfs -rm -r /user/anabig114249/hive/warehouse/dept_manager
hdfs dfs -rm -r /user/anabig114249/hive/warehouse/employees
hdfs dfs -rm -r /user/anabig114249/hive/warehouse/salaries
hdfs dfs -rm -r /user/anabig114249/hive/warehouse/titles


hdfs dfs -rm -r   /user/anabig114249/departments.avsc 
hdfs dfs -rm -r   /user/anabig114249/dept_emp.avsc 
hdfs dfs -rm -r  /user/anabig114249/dept_manager.avsc 
hdfs dfs -rm -r  /user/anabig114249/employees.avsc 
hdfs dfs -rm -r  /user/anabig114249/salaries.avsc 
hdfs dfs -rm -r  /user/anabig114249/titles.avsc 

hdfs dfs -rm -r /user/anabig114249/hive/warehouse

hdfs dfs -rm -r /user/anabig114249/hive/warehouse2

#**dislay the list of databases in mysql
sqoop list-databases --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306 --username anabig114249 --password Bigdata123

#**dislay the list of tables in the databases in  mysql
sqoop list-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114249 --username anabig114249 --password Bigdata123

#** to import all tables into hdfs avrodatafile format using sqoop
sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114249 --username anabig114249 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114249/hive/warehouse --driver com.mysql.jdbc.Driver --m 1


#** to import all tables into hdfs parquetfile format  using sqoop
sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114249 --username anabig114249 --password Bigdata123 --compression-codec=snappy --as-parquetfile --warehouse-dir=/user/anabig114249/hive/warehouse2 --driver com.mysql.jdbc.Driver --m 1

#sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114249 --username anabig114249 --password Bigdata123 --driver  com.mysql.jdbc.Driver  --table   dept_emp --target-dir =/user/anabig114249/hive/warehouse

#**to check data is imported to hdfs

hdfs dfs -ls /user/anabig114249/hive/warehouse

#**to check schema is imported to local
ls -l *.avsc

#**create folder in hdfs schema
hadoop fs -mkdir /user/anabig114249/schema  

#**load data from local to hdfs



hadoop fs -put /home/anabig114249/departments.avsc 
hadoop fs -put /home/anabig114249/dept_emp.avsc 
hadoop fs -put /home/anabig114249/dept_manager.avsc 
hadoop fs -put /home/anabig114249/employees.avsc 
hadoop fs -put /home/anabig114249/salaries.avsc 
hadoop fs -put /home/anabig114249/titles.avsc 



