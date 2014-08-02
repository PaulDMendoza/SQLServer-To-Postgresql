SQLServer-To-Postgresql
=======================

Convert a SQL Server schema to PostgreSQL schema. 

This is a super simple converter. It is coded to what I needed to make my very simple schema work. It only does tables, primary keys, FKs and basic indexes.

It also handles a couple types of default values but nothing much more complex than that. 

It doesn't do sprocs or views or any other objects. This is probably a good first start to doing your conversion. 

Submit pull requests if you make any changes to this application and I'll be happy to accept them.

Formatting Logic
-----------------
All column names are put in quotes and lower cased because Postgresql standard is lowercase and is case sensitive where as SQL Server isn't. Also, the quotes are to handle reserved key words.


