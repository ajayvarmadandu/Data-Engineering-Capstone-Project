
drop table if exists departments;

CREATE EXTERNAL TABLE departments
STORED AS AVRO LOCATION 'hdfs:///user/anabig114249/hive/warehouse/departments'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114249/departments.avsc');

drop table if exists dept_emp;

CREATE EXTERNAL TABLE dept_emp
STORED AS AVRO LOCATION 'hdfs:///user/anabig114249/hive/warehouse/dept_emp'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114249/dept_emp.avsc');

drop table if exists dept_manager;

CREATE EXTERNAL TABLE dept_manager
STORED AS AVRO LOCATION 'hdfs:///user/anabig114249/hive/warehouse/dept_manager'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114249/dept_manager.avsc');

drop table if exists employees;

CREATE EXTERNAL TABLE employees
STORED AS AVRO LOCATION 'hdfs:///user/anabig114249/hive/warehouse/employees'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114249/employees.avsc');


drop table if exists salaries;

CREATE EXTERNAL TABLE salaries
STORED AS AVRO LOCATION 'hdfs:///user/anabig114249/hive/warehouse/salaries'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114249/salaries.avsc');


drop table if exists titles;

CREATE EXTERNAL TABLE titles
STORED AS AVRO LOCATION 'hdfs:///user/anabig114249/hive/warehouse/titles'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114249/titles.avsc');



select * from departments limit 6;

select * from dept_emp limit 6;

select * from dept_manager limit 6;

select * from employees limit 6;

select * from salaries limit 6;

select * from titles limit 6;



--Exploratory Data Analysis
--(emp_no VARCHAR(20),emp_title_id VARCHAR(20),birth_date VARCHAR(20),first_name VARCHAR(20),last_name VARCHAR(20),sex VARCHAR(20),hire_date VARCHAR(20),no_of_projects VARCHAR(20),Last_performance_rating VARCHAR(20),left_ VARCHAR(20),last_date VARCHAR(20));


--1. A list showing employee number, last name, first name, sex, and salary for each employee

--1. A list showing first name, last name, and hire date for employees who were hired in 1986.

select t1.emp_no,t2.emp_no,last_name,first_name,sex,t2.salary
from employees t1 
inner join salaries t2 on t1.emp_no=t2.emp_no  
limit 6 ;


select first_name,last_name,hire_date,date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'MM/dd/yyyy')),'yyyy-MM-dd') as hire_date1
from employees where date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'MM/dd/yyyy')),'yyyy') = '1986' ;




--2. A list showing the manager of each department with the following information: department number, department name, the manager's employee number, last name, first name.



SELECT departments.dept_no, departments.dept_name, dept_manager.emp_no, employees.last_name, employees.first_name
FROM departments
JOIN dept_manager
ON departments.dept_no = dept_manager.dept_no
JOIN employees
ON dept_manager.emp_no = employees.emp_no;


--3. A list showing the department of each employee with the following information: employee number, last name, first name, and department name.employee number, last name, first name, and department name.

--employees - emp_no	emp_title_id	birth_date	first_name	last_name	sex	hire_date	no_of_projects	Last_performance_rating	left	last_date
--dept_emp - emp_no	dept_no
--departments - dept_no	dept_name



select employees.emp_no, employees.last_name, employees.first_name, departments.dept_name 
from  departments
inner join  dept_emp on departments.dept_no = dept_emp.dept_no
inner join employees on dept_emp.emp_no = employees.emp_no




--4. A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.“

select 	first_name,last_name,sex from employees 
where first_name = "Hercules" and last_name like 'B%';


--5. A list showing all employees in the Sales department, including their employee number, last name, first name, and department name.

select employees.emp_no, employees.last_name, employees.first_name, departments.dept_name 
from  departments
inner join  dept_emp on departments.dept_no = dept_emp.dept_no
inner join employees on dept_emp.emp_no = employees.emp_no
where dept_name = '"Sales"';




--6. A list showing all employees in the Sales and Development departments, including their employee number, last name, first name, and department name.

select employees.emp_no, employees.last_name, employees.first_name, departments.dept_name 
from  departments
inner join  dept_emp on departments.dept_no = dept_emp.dept_no
inner join employees on dept_emp.emp_no = employees.emp_no
where departments.dept_name = '"Sales"' or departments.dept_name = '"development"';


--7. A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each last name

select last_name,count(last_name) count_ from employees group by last_name order by count_ desc;


--8. Histogram to show the salary distribution among the employees

select t1.emp_no,t2.emp_no,last_name,first_name,sex,t2.salary
from employees t1 
inner join salaries t2 on t1.emp_no=t2.emp_no  
limit 6 ;

--9. Bar graph to show the Average salary per title (designation)

select t1.title, avg(t3.salary) as avg_salary
from titles t1 
inner join employees t2 on t1.title_id = t2.emp_title_id
inner join salaries t3 on t2.emp_no=t3.emp_no  
group by t1.title ;

--10. Calculate employee tenure & show the tenure distribution among the employees


select employees.emp_no, employees.last_name, employees.first_name,  2000 - date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'MM/dd/yyyy')),'yyyy') as tenure  from employees ;



