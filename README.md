The project is written in databricks, therefore might have some pyspark sql syntax and databricks notebook syntax. 
I made some assumptions on how to dedup the transaction table, which is used in pt1 and pt2 analysis. Hope that make sense. 
When join the user table, transaction table and product table, there are very limited records that can be joined, probably due to limited data provided. The pt2 result are based on the logic between the table. 
