#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext('local','Spark SQL')
Sqlcontext = SQLContext(sc)


# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataframeExercise").getOrCreate()


# In[3]:


# department dept_emp dept_manager employees salaries titles


# In[4]:


department = Sqlcontext.read.parquet("hdfs://nameservice1/user/anabig114249/hive/warehouse2/departments/be707ba0-3c17-416d-bb22-4ba252bb25d3.parquet")


# In[5]:


dept_emp = Sqlcontext.read.parquet("hdfs://nameservice1/user/anabig114249/hive/warehouse2/dept_emp/723a6614-4627-44b0-9e26-d151a38309be.parquet")


# In[6]:


dept_manager = Sqlcontext.read.parquet("hdfs://nameservice1/user/anabig114249/hive/warehouse2/dept_manager/5c5b9dee-8fe7-480b-8229-7834c51b824a.parquet")


# In[7]:


employees = Sqlcontext.read.parquet("hdfs://nameservice1/user/anabig114249/hive/warehouse2/employees/27e30786-e97b-4d1d-b3e7-219fe288b51d.parquet")


# In[8]:


salaries = Sqlcontext.read.parquet("hdfs://nameservice1/user/anabig114249/hive/warehouse2/salaries/fa7048d2-d613-4e08-94c7-02e18e93a1b8.parquet")


# In[9]:


titles = Sqlcontext.read.parquet("hdfs://nameservice1/user/anabig114249/hive/warehouse2/titles/906d7c87-71b9-4627-ba31-ac26683b1128.parquet")


# In[ ]:





# In[10]:


department.show(5)


# In[11]:


dept_emp.show(5)


# In[12]:


dept_manager.show(5)


# In[13]:


employees.show(5)


# In[14]:


salaries.show(5)


# In[15]:


titles.show(5)


# In[16]:


department.createTempView("departments_sql")
dept_emp.createTempView("dept_emp_sql")
dept_manager.createTempView("dept_manager_sql") 
employees.createTempView("employees_sql") 
salaries.createTempView("salaries_sql") 
titles.createTempView("titles_sql")


# ### 1. A list showing employee number, last name, first name, sex, and salary for each employee

# In[17]:



spark.sql('select t1.emp_no,t2.emp_no,last_name,first_name,sex,t2.salary from employees_sql t1 inner join salaries_sql t2 on t1.emp_no=t2.emp_no ').show()


# ## 1. A list showing first name, last name, and hire date for employees who were hired in 1986.

# In[ ]:





# In[18]:


spark.sql("select first_name,last_name,hire_date,date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'MM/dd/yyyy')),'yyyy-MM-dd') as hire_date1 from employees_sql where date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'MM/dd/yyyy')),'yyyy') = '1986'").show()


# ### 2. A list showing the manager of each department with the following information: department number, department name, the manager's employee number, last name, first name.

# In[ ]:





# In[19]:


spark.sql("SELECT departments_sql.dept_no, departments_sql.dept_name, dept_manager_sql.emp_no, employees_sql.last_name, employees_sql.first_name FROM departments_sql JOIN dept_manager_sql ON departments_sql.dept_no = dept_manager_sql.dept_no JOIN employees_sql ON dept_manager_sql.emp_no = employees_sql.emp_no").show()


# ### 3. A list showing the department of each employee with the following information: employee number, last name, first name, and department name.

# In[20]:


spark.sql("select employees_sql.emp_no, employees_sql.last_name, employees_sql.first_name, departments_sql.dept_name from  departments_sql inner join  dept_emp_sql on departments_sql.dept_no = dept_emp_sql.dept_no inner join employees_sql on dept_emp_sql.emp_no = employees_sql.emp_no").show()


# ### 4. A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.“
# 

# In[21]:


spark.sql("select first_name,last_name,sex from employees_sql where first_name = 'Hercules' and last_name like 'B%'").show()


# ### 5. A list showing all employees in the Sales department, including their employee number, last name, first name, and department name.
# 

# In[ ]:





# In[22]:


spark.sql("select employees_sql.emp_no, employees_sql.last_name, employees_sql.first_name, departments_sql.dept_name from  departments_sql inner join  dept_emp_sql on departments_sql.dept_no = dept_emp_sql.dept_no inner join employees_sql on dept_emp_sql.emp_no = employees_sql.emp_no where dept_name like '%Sales%'").show()


# ### 6. A list showing all employees in the Sales and Development departments, including their employee number, last name, first name, and department name.
# 

# In[ ]:





# In[23]:


spark.sql("select employees_sql.emp_no, employees_sql.last_name, employees_sql.first_name, departments_sql.dept_name from  departments_sql inner join  dept_emp_sql on departments_sql.dept_no = dept_emp_sql.dept_no inner join employees_sql on dept_emp_sql.emp_no = employees_sql.emp_no where dept_name like '%Sales%' or dept_name like '%development%'").show()


# ### --7. A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each last name
# 

# In[24]:


spark.sql("select last_name,count(last_name) count_ from employees_sql group by last_name order by count_ desc").show()


# ### 8. Histogram to show the salary distribution among the employees

# In[ ]:





# In[25]:


spark.sql("select t1.emp_no,t2.emp_no,last_name,first_name,sex,t2.salary from employees_sql t1 inner join salaries_sql t2 on t1.emp_no=t2.emp_no  ").show()


# In[ ]:





# ### 9. Bar graph to show the Average salary per title (designation)

# In[ ]:





# In[26]:


spark.sql("select t1.title, avg(t3.salary) as avg_salary from titles_sql t1 inner join employees_sql t2 on t1.title_id = t2.emp_title_id inner join salaries_sql t3 on t2.emp_no=t3.emp_no group by t1.title").show()


# ### 10. Calculate employee tenure & show the tenure distribution among the employees
# 

# In[ ]:





# In[ ]:





# In[ ]:





# In[27]:


data_all =spark.sql("select t1.dept_name,t2.dept_no,t3.birth_date,t3.emp_no,t3.emp_title_id,t3.first_name,t3.hire_date,t3.last_date,t3.last_name,t3.last_performance_rating,t3.left_,t3.no_of_projects,t3.sex,t4.salary,t5.title from  departments_sql t1 inner join  dept_emp_sql t2 on t1.dept_no = t2.dept_no inner join employees_sql t3 on t2.emp_no = t3.emp_no inner join salaries_sql t4 on t4.emp_no = t3.emp_no inner join titles_sql t5 on t5.title_id = t3.emp_title_id")


# In[28]:


data_all.show()


# In[29]:


data_all.columns


# In[30]:


final = data_all
for col in data_all.columns:
 final = data_all.withColumnRenamed(col,col.replace(" ", "_"))


# In[31]:


final.show()


# In[32]:


final.createTempView("final_sql")


# In[33]:


spark.sql('select distinct left_ from final_sql').show()


# In[34]:


final = final.withColumn("no_of_projects", final.no_of_projects.cast('int'))
final = final.withColumn("salary", final.no_of_projects.cast('int'))
final = final.withColumn("left_", final.left_.cast('int'))


# In[ ]:





# In[35]:


final.printSchema()


# In[36]:


continuous_features = [
 'no_of_projects',
 'salary']


# In[37]:


final.show()


# In[38]:


categorical_features = ['dept_name',
 'last_performance_rating',
 'sex',
 'title']


# In[39]:


y = ['left_']


# In[40]:


#Encoding all categorical features
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, PolynomialExpansion, VectorIndexer


# In[41]:


# create object of StringIndexer class and specify input and output column
SI_dept_name = StringIndexer(inputCol='dept_name',outputCol='dept_name_Index')
SI_last_performance_rating = StringIndexer(inputCol='last_performance_rating',outputCol='last_performance_rating_Index')
SI_sex = StringIndexer(inputCol='sex',outputCol='sex_Index')
SI_title = StringIndexer(inputCol='title',outputCol='title_Index')


# In[42]:



# transform the data
final = SI_dept_name.fit(final).transform(final)
final = SI_last_performance_rating.fit(final).transform(final)
final = SI_sex.fit(final).transform(final)
final = SI_title.fit(final).transform(final)



# In[43]:



# view the transformed data
final.select('dept_name', 'dept_name_Index', 'last_performance_rating', 'last_performance_rating_Index', 'sex', 'sex_Index','title','title_Index').show(10)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[44]:


assesmble=VectorAssembler(inputCols=['no_of_projects',
 'salary',
 'dept_name_Index',
 'last_performance_rating_Index',
 'sex_Index',
 'title_Index'],outputCol='features')


# In[45]:


final1=assesmble.transform(final)


# In[46]:


final1.show()


# In[ ]:





# In[47]:


df=final1.select('features','left_')


# In[48]:


df.printSchema()


# In[49]:


(train, test) = df.randomSplit([.7,.3])


# In[50]:


train.show(2)


# In[51]:


test.show(2)


# In[52]:


from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score


# In[ ]:





# #  RandomForestClassifier

# In[ ]:





# In[53]:


rf = RandomForestClassifier(labelCol='left_', 
                            featuresCol='features',
                            maxDepth=5)


# In[54]:


model = rf.fit(train)


# In[55]:


rf_predictions = model.transform(test)


# In[56]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[57]:


multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'left_', metricName = 'accuracy')


# In[58]:


print('Random Forest classifier Accuracy:', multi_evaluator.evaluate(rf_predictions))


# In[ ]:





# ## LogisticRegression

# In[ ]:





# In[59]:


lr = LogisticRegression(featuresCol ='features', labelCol ='left_', maxIter=10)


# In[60]:


lrModel = lr.fit(train)


# In[61]:


lr_predictions = lrModel.transform(test)


# In[62]:


log_multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'left_', metricName = 'accuracy')


# In[63]:



print('LogisticRegression Accuracy:', log_multi_evaluator.evaluate(lr_predictions))


# In[ ]:





# ## DecisionTreeClassifier

# In[64]:



from pyspark.ml.classification import DecisionTreeClassifier


# In[65]:


dt = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'left_', maxDepth = 3)


# In[66]:


dtModel = dt.fit(train)


# In[67]:


dt_predictions = dtModel.transform(test)


# In[68]:


multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'left_', metricName = 'accuracy')


# In[69]:



print('Decision Tree Accuracy:', multi_evaluator.evaluate(dt_predictions))


# In[ ]:





# In[70]:


spark.stop()     


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




