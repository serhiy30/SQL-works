# Jira Worklog Aggregation in Databricks 📊
---
📄 ENG: Overview
---
This repository contains SQL scripts for aggregating Jira worklog data in **Databricks**. The scripts process worklogs, tasks, epics, and sub-tasks to generate a consolidated view of hours spent by users across different Jira issues. ⏱️

## Overview 🌟
The main goal of this project is to create a **gold-level table** in Databricks that consolidates worklog data from Jira. It handles multiple levels of Jira hierarchy:
- **Epic** 🏢
- **Task / Story** 📌
- **Sub-task** 📝

Additionally, it aggregates time spent by each author and handles multiple expense types in the data 💰.

## Data Sources 🗂️
The scripts use the following source tables in the `main.data.jira.silver.data` schema:
- `tempo_worklogs` – raw worklogs from Jira Tempo ⏱️
- `users` – Jira user information 👤
- `change_logs` – Jira issue change logs 🔄
- `jira_issues` – Jira issues, epics, and sub-tasks 🗂️
- `projects` – Jira project metadata 🏗️
- `types_for_reporting` – mapping of Jira issue types to report categories 📊

## SQL Tables 📝
The main output table is:

**`main.data.jira.gold.taskdata`** – contains aggregated worklog data per user, per Jira issue, with the following key columns:
- `Year`, `Month`, `START_DATE` 📅
- `TEMPO_Author_ID`, `ASSIGNEE` 👤
- `Epic_ID`, `Epic_SUMMARY` 🏢
- `Task_ID`, `Task_SUMMARY` 📌
- `Sub_task_ID`, `Sub_task_SUMMARY` 📝
- `Hours_spent`, `Seconds_spent` ⏱️
- `Expense_Type` 💰

## Notes ⚠️

The SQL script includes UNION ALL of multiple levels of Jira issues to ensure proper aggregation.

Null values for assignees or authors are replaced with 'Unassigned' 👤

Expense types are resolved hierarchically from three possible columns (Expense_Type_1, Expense_Type_2, Expense_Type_3) 💰

Output is ordered by Year, Month, and user/issue hierarchy 📅
---
📄 DE: Übersicht
---
Dieses Repository enthält SQL-Skripte zum Aggregieren von Jira-Arbeitsprotokoll-Daten in **Databricks**. Die Skripte verarbeiten Arbeitsprotokolle, Aufgaben, Epics und Unteraufgaben, um eine konsolidierte Ansicht der von Benutzern für verschiedene Jira-Issues aufgewendeten Stunden zu erstellen. ⏱️

## Übersicht 🌟
Das Hauptziel dieses Projekts ist die Erstellung einer **Gold-Level-Tabelle** in Databricks, die Arbeitsprotokoll-Daten aus Jira konsolidiert. Es verarbeitet mehrere Ebenen der Jira-Hierarchie:
- **Epic** 🏢
- **Aufgabe / Story** 📌
- **Unteraufgabe** 📝

Darüber hinaus aggregiert es die von jedem Autor aufgewendete Zeit und verarbeitet mehrere Ausgabentypen in den Daten 💰.

## Datenquellen 🗂️
Die Skripte verwenden die folgenden Quelltabellen im Schema `main.data.jira.silver.data`:
- `tempo_worklogs` – Rohdaten aus Jira Tempo ⏱️
- `users` – Jira-Benutzerinformationen 👤
- `change_logs` – Jira-Änderungsprotokolle 🔄
- `jira_issues` – Jira-Vorgänge, Epics und Unteraufgaben 🗂️
- `projects` – Jira-Projektmetadaten 🏗️
- `types_for_reporting` – Zuordnung von Jira-Issue-Typen zu Berichtskategorien 📊

## SQL-Tabellen 📝
Die Hauptausgabetabelle lautet:

**`main.data.jira.gold.taskdata`** – enthält aggregierte Arbeitsprotokoll-Daten pro Benutzer und pro Jira-Issue mit den folgenden Schlüsselspalten:
- `Jahr`, `Monat`, `START_DATE` 📅
- `TEMPO_Author_ID`, `ASSIGNEE` 👤
- `Epic_ID`, `Epic_SUMMARY` 🏢
- `Task_ID`, `Task_SUMMARY` 📌
- `Sub_task_ID`, `Sub_task_SUMMARY` 📝
- `Hours_spent`, `Seconds_spent` ⏱️
- `Expense_Type` 💰

## Hinweise ⚠️
Das SQL-Skript enthält UNION ALL für mehrere Ebenen von Jira-Vorgängen, um eine korrekte Aggregation sicherzustellen.

Nullwerte für Bearbeiter oder Autoren werden durch „Nicht zugewiesen“ ersetzt 👤.

Ausgabentypen werden hierarchisch aus drei möglichen Spalten (Expense_Type_1, Expense_Type_2, Expense_Type_3) aufgelöst 💰.

Die Ausgabe ist nach Jahr, Monat und Benutzer-/Issue-Hierarchie sortiert 📅.
