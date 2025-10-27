-- ===========================================
-- Query to compare replacement products with alternative products
-- Calculates price differences, margin differences, shipping differences, etc.
-- Written for Databricks (Spark SQL)
-- ===========================================

SELECT
 Tempo.Year,
 Tempo.Month,
 Tempo.TEMPO_Author_ID,
 Tempo.ASSIGNEE, 
 Tempo.Epic_ID,
 Tempo.Epic_SUMMARY,
 Tempo.Task_ID,
 Tempo.Task_SUMMARY,
 Tempo.Sub_task_ID,
 Tempo.Sub_task_SUMMARY,
 SUM(Tempo.TIMESPENTHRS) AS Hours_spent,
 SUM(Tempo.TIMESPENTSECONDS) AS Seconds_spent,
 date_format(Tempo.START_DATE, 'yyyy-MM-dd') AS START_DATE,
 CASE 
    WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
         AND (Tempo.Expense_Type_2 IS NULL OR Tempo.Expense_Type_2 = '') 
         THEN Tempo.Expense_Type_1
    WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
         AND (Tempo.Expense_Type_2 IS NOT NULL AND Tempo.Expense_Type_2 != '') 
         THEN Tempo.Expense_Type_2
    ELSE Tempo.Expense_Type_3 
 END AS Expense_Type

FROM
(Select 
    month(tmp.STARTDATE) AS Month,
    year(tmp.STARTDATE) AS Year,
    tmp.STARTDATE AS START_DATE,
    CASE WHEN u.DISPLAYNAME IS null THEN 'Unassigned' ELSE u.DISPLAYNAME END  AS TEMPO_Author_ID, 
    CASE WHEN ji.ASSIGNEE IS null THEN 'Unassigned' ELSE ji.ASSIGNEE END AS ASSIGNEE, 
    tmp.ISSUEID,
    ji.IO_JIRA_ACCOUNT AS Expense_Type_1,
    ji.PROJECT_KEY AS PARENT,
    log.ISSUEKEY AS Epic_ID,
    ji.SUMMARY AS Epic_SUMMARY,
    ji.ISSUETYPE,
    '' AS Task_ID,
    '' AS Task_SUMMARY,
    '' AS Expense_Type_2,
    '' AS Sub_task_ID,
    '' AS Sub_task_SUMMARY,
    '' AS Expense_Type_3,
    tmp.DESCRIPTION,
    tmp.TIMESPENTSECONDS / 3600 AS TIMESPENTHRS,
    tmp.TIMESPENTSECONDS AS TIMESPENTSECONDS,
    tmp.AUTHORACCOUNTID,
    tmp.TEMPOWORKLOGID
    
FROM main.data.jira.silver.data.tempo_worklogs tmp
    LEFT JOIN main.data.jira.silver.data.users u ON tmp.AUTHORACCOUNTID = u.ACCOUNTID
    LEFT JOIN main.data.jira.silver.data.change_logs log ON log.ISSUEID = tmp.ISSUEID
    LEFT JOIN main.data.jira.silver.data.jira_issues ji  ON ji.KEY = log.ISSUEKEY
WHERE  
   ji.ISSUETYPE IN (SELECT ISSUETYPE_NAME FROM main.data.jira.silver.data.types_for_reporting WHERE DBX_report_column = 'Project/Epic')
   AND tmp.DELETEDAT IS NULL and tmp.isdeleted = false

GROUP BY Month, Year, START_DATE, TEMPO_Author_ID, ji.ASSIGNEE, tmp.ISSUEID, ji.PROJECT_KEY, Epic_ID, Epic_SUMMARY, ji.ISSUETYPE, tmp.AUTHORACCOUNTID, TIMESPENTHRS, 
 TIMESPENTSECONDS, tmp.DESCRIPTION, tmp.TEMPOWORKLOGID, Expense_Type_1, Expense_Type_2, Expense_Type_3

UNION ALL 

Select 
    month(tmp.STARTDATE) AS Month,
    year(tmp.STARTDATE) AS Year,
   tmp.STARTDATE AS START_DATE,
    CASE WHEN u.DISPLAYNAME IS null THEN 'Unassigned' ELSE u.DISPLAYNAME END  AS TEMPO_Author_ID, 
    CASE WHEN ji.ASSIGNEE IS null THEN 'Unassigned' ELSE ji.ASSIGNEE END AS ASSIGNEE, 
    tmp.ISSUEID,
     jit.IO_JIRA_ACCOUNT AS Expense_Type_1,
    CASE WHEN jit.ISSUETYPE IS NULL THEN 'Epic'ELSE jit.ISSUETYPE END AS PARENT,
    CASE WHEN ji.PARENT IS NULL THEN ji.PROJECT_KEY ELSE ji.PARENT END AS Epic_ID,
    CASE WHEN jit.SUMMARY IS NULL THEN proj.NAME ELSE jit.SUMMARY END AS Epic_SUMMARY,
    ji.ISSUETYPE,
    log.ISSUEKEY AS Task_ID,
    ji.SUMMARY AS Task_SUMMARY,
    ji.IO_JIRA_ACCOUNT AS Expense_Type_2,
    '' AS Sub_task_ID,
    '' AS Sub_task_SUMMARY,
    '' AS Expense_Type_3,
    tmp.DESCRIPTION,
    tmp.TIMESPENTSECONDS / 3600 AS TIMESPENTHRS,
    tmp.TIMESPENTSECONDS AS TIMESPENTSECONDS,
    tmp.AUTHORACCOUNTID,
    tmp.TEMPOWORKLOGID
FROM
   main.data.jira.silver.data.tempo_worklogs tmp
 LEFT JOIN main.data.jira.silver.data.users u ON tmp.AUTHORACCOUNTID = u.ACCOUNTID
 LEFT JOIN main.data.jira.silver.data.change_logs log ON log.ISSUEID = tmp.ISSUEID
 LEFT JOIN main.data.jira.silver.data.jira_issues ji  ON ji.KEY = log.ISSUEKEY
 LEFT JOIN main.data.jira.silver.data.jira_issues jit  ON jit.KEY = ji.PARENT
 LEFT JOIN main.data.jira.silver.data.jira_issues jie  ON jie.KEY = jit.PARENT
 LEFT JOIN (SELECT PROJECTKEY, MAX(NAME) AS NAME FROM main.data.jira.silver.data.projects Group by all) AS proj ON ji.PROJECT_KEY = proj.PROJECTKEY
WHERE  
   ji.ISSUETYPE IN (SELECT ISSUETYPE_NAME FROM main.data.jira.silver.data.types_for_reporting WHERE DBX_report_column IN ('Task/Story', 'Other'))
   AND tmp.DELETEDAT IS NULL and tmp.isdeleted = false
 
GROUP BY Month, Year, START_DATE, TEMPO_Author_ID, ji.ASSIGNEE,tmp.ISSUEID, CASE WHEN jit.ISSUETYPE IS NULL THEN 'Epic'ELSE jit.ISSUETYPE END, Epic_ID, Epic_SUMMARY, 
 ji.ISSUETYPE, Task_ID, Task_SUMMARY, tmp.AUTHORACCOUNTID, TIMESPENTHRS, TIMESPENTSECONDS, tmp.DESCRIPTION, tmp.TEMPOWORKLOGID, Expense_Type_1, Expense_Type_2, Expense_Type_3

UNION ALL

Select 
    month(tmp.STARTDATE) AS Month,
    year(tmp.STARTDATE) AS Year,
    tmp.STARTDATE AS START_DATE,
    CASE WHEN u.DISPLAYNAME IS null THEN 'Unassigned' ELSE u.DISPLAYNAME END  AS TEMPO_Author_ID, 
    CASE WHEN ji.ASSIGNEE IS null THEN 'Unassigned' ELSE ji.ASSIGNEE END AS ASSIGNEE, 
    tmp.ISSUEID,
    jie.IO_JIRA_ACCOUNT AS Expense_Type_1,
    jit.ISSUETYPE AS PARENT,
    CASE WHEN jit.PARENT IS NULL THEN jit.PROJECT_KEY ELSE jit.PARENT END AS Epic_ID,
    CASE WHEN jie.SUMMARY IS NULL THEN jit.PROJECT ELSE jie.SUMMARY END AS Epic_SUMMARY,
    ji.ISSUETYPE,
    CASE WHEN ji.PARENT IS NULL THEN ji.PROJECT_KEY ELSE ji.PARENT END AS Task_ID,
    jit.SUMMARY AS Task_SUMMARY,
    jit.IO_JIRA_ACCOUNT AS Expense_Type_2,
    log.ISSUEKEY AS Sub_task_ID,
    ji.SUMMARY AS Sub_task_SUMMARY,
    ji.IO_JIRA_ACCOUNT AS Expense_Type_3,
    tmp.DESCRIPTION,
    tmp.TIMESPENTSECONDS / 3600 AS TIMESPENTHRS,
    tmp.TIMESPENTSECONDS AS TIMESPENTSECONDS,
    tmp.AUTHORACCOUNTID,
    tmp.TEMPOWORKLOGID

FROM
   main.data.jira.silver.data.tempo_worklogs tmp
   LEFT JOIN main.data.jira.silver.data.users u ON tmp.AUTHORACCOUNTID = u.ACCOUNTID
   LEFT JOIN main.data.jira.silver.data.change_logs log ON log.ISSUEID = tmp.ISSUEID
   LEFT JOIN main.data.jira.silver.data.jira_issues ji  ON ji.KEY = log.ISSUEKEY
   LEFT JOIN main.data.jira.silver.data.jira_issues jit  ON jit.KEY = ji.PARENT
   LEFT JOIN main.data.jira.silver.data.jira_issues jie  ON jie.KEY = jit.PARENT
WHERE  
   ji.ISSUETYPE IN (SELECT ISSUETYPE_NAME FROM main.data.jira.silver.data.types_for_reporting WHERE DBX_report_column IN ('Sub-Task'))
   AND tmp.DELETEDAT IS NULL and tmp.isdeleted = false
   
GROUP BY Month, Year, START_DATE, TEMPO_Author_ID, ji.ASSIGNEE, tmp.ISSUEID, jit.ISSUETYPE, Epic_ID, Epic_SUMMARY, ji.ISSUETYPE, Task_ID, Task_SUMMARY, Sub_task_ID, 
 Sub_task_SUMMARY, tmp.AUTHORACCOUNTID, TIMESPENTHRS, TIMESPENTSECONDS, tmp.DESCRIPTION, tmp.TEMPOWORKLOGID, Expense_Type_1, Expense_Type_2, Expense_Type_3
ORDER BY TEMPO_Author_ID) As Tempo

GROUP BY 
 Tempo.Month,
 Tempo.Year,
 START_DATE,
 Tempo.TEMPO_Author_ID, 
 Tempo.ASSIGNEE, 
 Tempo.Epic_ID,
 Tempo.Epic_SUMMARY,
 Tempo.Task_ID,
 Tempo.Task_SUMMARY,
 Tempo.Sub_task_ID,
 Tempo.Sub_task_SUMMARY,
  CASE 
    WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
         AND (Tempo.Expense_Type_2 IS NULL OR Tempo.Expense_Type_2 = '') 
         THEN Tempo.Expense_Type_1
    WHEN (Tempo.Expense_Type_3 IS NULL OR Tempo.Expense_Type_3 = '') 
         AND (Tempo.Expense_Type_2 IS NOT NULL AND Tempo.Expense_Type_2 != '') 
         THEN Tempo.Expense_Type_2
    ELSE Tempo.Expense_Type_3 
    END 

Order by Year Desc, Month Desc, Tempo.ASSIGNEE, Tempo.Epic_ID, Tempo.Task_ID, Tempo.Sub_task_ID;
