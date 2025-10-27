-- ===========================================
-- Query to compare replacement products with alternative products
-- Calculates price differences, margin differences, shipping differences, etc.
-- Written for Databricks (Spark SQL)
-- ===========================================

# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.data.jira.gold.taskdata
# MAGIC SELECT
# MAGIC  Tempo.Year,
# MAGIC  Tempo.Month,
# MAGIC  Tempo.TEMPO_Author_ID,
# MAGIC  Tempo.ASSIGNEE, 
# MAGIC  Tempo.Epic_ID,
# MAGIC  Tempo.Epic_SUMMARY,
# MAGIC  Tempo.Task_ID,
# MAGIC  Tempo.Task_SUMMARY,
# MAGIC  Tempo.Sub_task_ID,
# MAGIC  Tempo.Sub_task_SUMMARY,
# MAGIC  SUM(Tempo.TIMESPENTHRS) AS Hours_spent,
# MAGIC  SUM(Tempo.TIMESPENTSECONDS) AS Seconds_spent,
# MAGIC  date_format(Tempo.START_DATE, 'yyyy-MM-dd') AS START_DATE,
# MAGIC  CASE 
# MAGIC     WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
# MAGIC          AND (Tempo.Expense_Type_2 IS NULL OR Tempo.Expense_Type_2 = '') 
# MAGIC          THEN Tempo.Expense_Type_1
# MAGIC     WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
# MAGIC          AND (Tempo.Expense_Type_2 IS NOT NULL AND Tempo.Expense_Type_2 != '') 
# MAGIC          THEN Tempo.Expense_Type_2
# MAGIC     ELSE Tempo.Expense_Type_3 
# MAGIC  END AS Expense_Type
# MAGIC
# MAGIC FROM
# MAGIC (Select 
# MAGIC     month(tmp.STARTDATE) AS Month,
# MAGIC     year(tmp.STARTDATE) AS Year,
# MAGIC     tmp.STARTDATE AS START_DATE,
# MAGIC     CASE WHEN u.DISPLAYNAME IS null THEN 'Unassigned' ELSE u.DISPLAYNAME END  AS TEMPO_Author_ID, 
# MAGIC     CASE WHEN ji.ASSIGNEE IS null THEN 'Unassigned' ELSE ji.ASSIGNEE END AS ASSIGNEE, 
# MAGIC     tmp.ISSUEID,
# MAGIC     ji.IO_JIRA_ACCOUNT AS Expense_Type_1,
# MAGIC     ji.PROJECT_KEY AS PARENT,
# MAGIC     log.ISSUEKEY AS Epic_ID,
# MAGIC     ji.SUMMARY AS Epic_SUMMARY,
# MAGIC     ji.ISSUETYPE,
# MAGIC     '' AS Task_ID,
# MAGIC     '' AS Task_SUMMARY,
# MAGIC     '' AS Expense_Type_2,
# MAGIC     '' AS Sub_task_ID,
# MAGIC     '' AS Sub_task_SUMMARY,
# MAGIC     '' AS Expense_Type_3,
# MAGIC     tmp.DESCRIPTION,
# MAGIC     tmp.TIMESPENTSECONDS / 3600 AS TIMESPENTHRS,
# MAGIC     tmp.TIMESPENTSECONDS AS TIMESPENTSECONDS,
# MAGIC     tmp.AUTHORACCOUNTID,
# MAGIC     tmp.TEMPOWORKLOGID
# MAGIC     
# MAGIC FROM main.data.jira.silver.data.tempo_worklogs tmp
# MAGIC     LEFT JOIN main.data.jira.silver.data.users u ON tmp.AUTHORACCOUNTID = u.ACCOUNTID
# MAGIC     LEFT JOIN main.data.jira.silver.data.change_logs log ON log.ISSUEID = tmp.ISSUEID
# MAGIC     LEFT JOIN main.data.jira.silver.data.jira_issues ji  ON ji.KEY = log.ISSUEKEY
# MAGIC WHERE  
# MAGIC    ji.ISSUETYPE IN (SELECT ISSUETYPE_NAME FROM main.data.jira.silver.data.types_for_reporting WHERE DBX_report_column = 'Project/Epic')
# MAGIC    AND tmp.DELETEDAT IS NULL and tmp.isdeleted = false
# MAGIC
# MAGIC GROUP BY Month, Year, START_DATE, TEMPO_Author_ID, ji.ASSIGNEE, tmp.ISSUEID, ji.PROJECT_KEY, Epic_ID, Epic_SUMMARY, ji.ISSUETYPE, tmp.AUTHORACCOUNTID, TIMESPENTHRS, 
# MAGIC  TIMESPENTSECONDS, tmp.DESCRIPTION, tmp.TEMPOWORKLOGID, Expense_Type_1, Expense_Type_2, Expense_Type_3
# MAGIC
# MAGIC UNION ALL 
# MAGIC
# MAGIC Select 
# MAGIC     month(tmp.STARTDATE) AS Month,
# MAGIC     year(tmp.STARTDATE) AS Year,
# MAGIC    tmp.STARTDATE AS START_DATE,
# MAGIC     CASE WHEN u.DISPLAYNAME IS null THEN 'Unassigned' ELSE u.DISPLAYNAME END  AS TEMPO_Author_ID, 
# MAGIC     CASE WHEN ji.ASSIGNEE IS null THEN 'Unassigned' ELSE ji.ASSIGNEE END AS ASSIGNEE, 
# MAGIC     tmp.ISSUEID,
# MAGIC      jit.IO_JIRA_ACCOUNT AS Expense_Type_1,
# MAGIC     CASE WHEN jit.ISSUETYPE IS NULL THEN 'Epic'ELSE jit.ISSUETYPE END AS PARENT,
# MAGIC     CASE WHEN ji.PARENT IS NULL THEN ji.PROJECT_KEY ELSE ji.PARENT END AS Epic_ID,
# MAGIC     CASE WHEN jit.SUMMARY IS NULL THEN proj.NAME ELSE jit.SUMMARY END AS Epic_SUMMARY,
# MAGIC     ji.ISSUETYPE,
# MAGIC     log.ISSUEKEY AS Task_ID,
# MAGIC     ji.SUMMARY AS Task_SUMMARY,
# MAGIC     ji.IO_JIRA_ACCOUNT AS Expense_Type_2,
# MAGIC     '' AS Sub_task_ID,
# MAGIC     '' AS Sub_task_SUMMARY,
# MAGIC     '' AS Expense_Type_3,
# MAGIC     tmp.DESCRIPTION,
# MAGIC     tmp.TIMESPENTSECONDS / 3600 AS TIMESPENTHRS,
# MAGIC     tmp.TIMESPENTSECONDS AS TIMESPENTSECONDS,
# MAGIC     tmp.AUTHORACCOUNTID,
# MAGIC     tmp.TEMPOWORKLOGID
# MAGIC FROM
# MAGIC    main.data.jira.silver.data.tempo_worklogs tmp
# MAGIC  LEFT JOIN main.data.jira.silver.data.users u ON tmp.AUTHORACCOUNTID = u.ACCOUNTID
# MAGIC  LEFT JOIN main.data.jira.silver.data.change_logs log ON log.ISSUEID = tmp.ISSUEID
# MAGIC  LEFT JOIN main.data.jira.silver.data.jira_issues ji  ON ji.KEY = log.ISSUEKEY
# MAGIC  LEFT JOIN main.data.jira.silver.data.jira_issues jit  ON jit.KEY = ji.PARENT
# MAGIC  LEFT JOIN main.data.jira.silver.data.jira_issues jie  ON jie.KEY = jit.PARENT
# MAGIC  LEFT JOIN (SELECT PROJECTKEY, MAX(NAME) AS NAME FROM main.data.jira.silver.data.projects Group by all) AS proj ON ji.PROJECT_KEY = proj.PROJECTKEY
# MAGIC WHERE  
# MAGIC    ji.ISSUETYPE IN (SELECT ISSUETYPE_NAME FROM main.data.jira.silver.data.types_for_reporting WHERE DBX_report_column IN ('Task/Story', 'Other'))
# MAGIC    AND tmp.DELETEDAT IS NULL and tmp.isdeleted = false
# MAGIC  
# MAGIC GROUP BY Month, Year, START_DATE, TEMPO_Author_ID, ji.ASSIGNEE,tmp.ISSUEID, CASE WHEN jit.ISSUETYPE IS NULL THEN 'Epic'ELSE jit.ISSUETYPE END, Epic_ID, Epic_SUMMARY, 
# MAGIC  ji.ISSUETYPE, Task_ID, Task_SUMMARY, tmp.AUTHORACCOUNTID, TIMESPENTHRS, TIMESPENTSECONDS, tmp.DESCRIPTION, tmp.TEMPOWORKLOGID, Expense_Type_1, Expense_Type_2, Expense_Type_3
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC Select 
# MAGIC     month(tmp.STARTDATE) AS Month,
# MAGIC     year(tmp.STARTDATE) AS Year,
# MAGIC     tmp.STARTDATE AS START_DATE,
# MAGIC     CASE WHEN u.DISPLAYNAME IS null THEN 'Unassigned' ELSE u.DISPLAYNAME END  AS TEMPO_Author_ID, 
# MAGIC     CASE WHEN ji.ASSIGNEE IS null THEN 'Unassigned' ELSE ji.ASSIGNEE END AS ASSIGNEE, 
# MAGIC     tmp.ISSUEID,
# MAGIC     jie.IO_JIRA_ACCOUNT AS Expense_Type_1,
# MAGIC     jit.ISSUETYPE AS PARENT,
# MAGIC     CASE WHEN jit.PARENT IS NULL THEN jit.PROJECT_KEY ELSE jit.PARENT END AS Epic_ID,
# MAGIC     CASE WHEN jie.SUMMARY IS NULL THEN jit.PROJECT ELSE jie.SUMMARY END AS Epic_SUMMARY,
# MAGIC     ji.ISSUETYPE,
# MAGIC     CASE WHEN ji.PARENT IS NULL THEN ji.PROJECT_KEY ELSE ji.PARENT END AS Task_ID,
# MAGIC     jit.SUMMARY AS Task_SUMMARY,
# MAGIC     jit.IO_JIRA_ACCOUNT AS Expense_Type_2,
# MAGIC     log.ISSUEKEY AS Sub_task_ID,
# MAGIC     ji.SUMMARY AS Sub_task_SUMMARY,
# MAGIC     ji.IO_JIRA_ACCOUNT AS Expense_Type_3,
# MAGIC     tmp.DESCRIPTION,
# MAGIC     tmp.TIMESPENTSECONDS / 3600 AS TIMESPENTHRS,
# MAGIC     tmp.TIMESPENTSECONDS AS TIMESPENTSECONDS,
# MAGIC     tmp.AUTHORACCOUNTID,
# MAGIC     tmp.TEMPOWORKLOGID
# MAGIC
# MAGIC FROM
# MAGIC    main.data.jira.silver.data.tempo_worklogs tmp
# MAGIC    LEFT JOIN main.data.jira.silver.data.users u ON tmp.AUTHORACCOUNTID = u.ACCOUNTID
# MAGIC    LEFT JOIN main.data.jira.silver.data.change_logs log ON log.ISSUEID = tmp.ISSUEID
# MAGIC    LEFT JOIN main.data.jira.silver.data.jira_issues ji  ON ji.KEY = log.ISSUEKEY
# MAGIC    LEFT JOIN main.data.jira.silver.data.jira_issues jit  ON jit.KEY = ji.PARENT
# MAGIC    LEFT JOIN main.data.jira.silver.data.jira_issues jie  ON jie.KEY = jit.PARENT
# MAGIC WHERE  
# MAGIC    ji.ISSUETYPE IN (SELECT ISSUETYPE_NAME FROM main.data.jira.silver.data.types_for_reporting WHERE DBX_report_column IN ('Sub-Task'))
# MAGIC    AND tmp.DELETEDAT IS NULL and tmp.isdeleted = false
# MAGIC    
# MAGIC GROUP BY Month, Year, START_DATE, TEMPO_Author_ID, ji.ASSIGNEE, tmp.ISSUEID, jit.ISSUETYPE, Epic_ID, Epic_SUMMARY, ji.ISSUETYPE, Task_ID, Task_SUMMARY, Sub_task_ID, 
# MAGIC  Sub_task_SUMMARY, tmp.AUTHORACCOUNTID, TIMESPENTHRS, TIMESPENTSECONDS, tmp.DESCRIPTION, tmp.TEMPOWORKLOGID, Expense_Type_1, Expense_Type_2, Expense_Type_3
# MAGIC ORDER BY TEMPO_Author_ID) As Tempo
# MAGIC
# MAGIC
# MAGIC GROUP BY 
# MAGIC  Tempo.Month,
# MAGIC  Tempo.Year,
# MAGIC  START_DATE,
# MAGIC  Tempo.TEMPO_Author_ID, 
# MAGIC  Tempo.ASSIGNEE, 
# MAGIC  Tempo.Epic_ID,
# MAGIC  Tempo.Epic_SUMMARY,
# MAGIC  Tempo.Task_ID,
# MAGIC  Tempo.Task_SUMMARY,
# MAGIC  Tempo.Sub_task_ID,
# MAGIC  Tempo.Sub_task_SUMMARY,
# MAGIC   CASE 
# MAGIC     WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
# MAGIC          AND (Tempo.Expense_Type_2 IS NULL OR Tempo.Expense_Type_2 = '') 
# MAGIC          THEN Tempo.Expense_Type_1
# MAGIC     WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
# MAGIC          AND (Tempo.Expense_Type_2 IS NOT NULL AND Tempo.Expense_Type_2 != '') 
# MAGIC          THEN Tempo.Expense_Type_2
# MAGIC     ELSE Tempo.Expense_Type_3 
# MAGIC     END 
# MAGIC
# MAGIC Order by Year Desc, Month Desc, Tempo.ASSIGNEE, Tempo.Epic_ID, Tempo.Task_ID, Tempo.Sub_task_ID;
