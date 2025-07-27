# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,Python List
# Lists are used to store multiple items in a single variable.
# List items are ordered, changeable, and allow duplicate values.
# List can store different data types
# List is mutable
List = [1,2,3,4,5,"Python","MSC(DS)",8,0]
print(List)
print(List[5])


# COMMAND ----------

# Remove a particular element
List.remove("Python")
print(List)

# Remove Last element from the list
List.pop()
print(List)

# COMMAND ----------

# Insert an element at the end

List.append("JaVA")
print(List)

# Insert an element at particular position

List.insert(3,"SAP")
print(List)


# COMMAND ----------

# COMMAND ----------

# User input list
a = []
for i in range (5):
    x=int(input("Please provide your number : "))
    a.append(x)
print(a)

# length of the list a

print(len(a))


# COMMAND ----------

# COMMAND ----------

# Nested List 

data = ['Samsung',[12,23,34],"Apple",[9,6,7]]
print(data[0])

print(data[1][0])


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Python Dictionary
# Dictionary is always a Key Value Pair

# Dictionary is mutable

# Dictionary is dynamic and it can grow and shrink as per requirement.

dict = {1:"Python",2:"Java",3:"C++",4:"C"}
print(dict)

# COMMAND ----------

# Print particular key value
print(dict[2])

# Creating a Dictionary
# with Mixed keys
Dict = {'Name': 'codeHack', 1: [1, 2, 3, 4]}
print("\nDictionary with the use of Mixed Keys: ")
print(Dict)

# Length of the Dictionary
print(len(dict))

# COMMAND ----------

# COMMAND ----------

# Take the user input for the dictionary

employee ={}

for i in range (5):
    name = input("Please enter the name : ")
    salary = input("Enter salary ammount :")
    employee[name] = salary
    
print(employee)

# COMMAND ----------

