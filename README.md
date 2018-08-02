## MIE1512_Data-Analytics_Final-Project

# The Effects of Diversity on Group Productivity and Member Withdrawal in GitHub

## I. Project Introduction

In this project, I mainly explore and discuss the relationship between group diversity and outcomes in GitHub projects. Based on the first paper I listed in my bibliography, I refer to its measurable variables and analysis model for Wikipedia datasets. Then combining with the second paper, I make some adjustments for GitHub groups.

I consider the effects of diversity from two aspects, tenure diversity and country diversity. They are also the independent variables. The tenure diversity of a project group is the coefficient of variation of Ti for all its members. Ti is tenure of each member which is defined as the number of days from the first event of a quarter (a 90-day period) to the end of the quarter. To get the coefficient of variation, we also need the mean value of members' tenure. For country diversity, we will firstly calculate the percentages of group members from each country. Then use Blau index to express the country diversity of this group. 

In this project, the corresponding dependent variables are group productivity and member withdrawal. We use the number of events from all group members to measure group productivity. Events here include pull-requests, commits, comments and issues. Member withdrawal of a project group is obtained from the difference of the number of members who have activities in this quarter and that of last quarter.

There are also some controlled variables that may have potential influnence on response variables like quarter index and project size. Quarter index will be included within tables of response variables to show their change among different quarters. Project size is defined as the number of members in the project. We will build its dataframe seperately.

The data source of my project is the GHTorrent datasets on Google Bigquery. According to definitions of variables stated above, I list the datasets and attributes chosen of this project in the data preparation section. After selecting and filering data on Bigquery, I export the prepared data as .csv files and download to local through Jupyter. Then upload them to Google drive. Next, import the data to Spark RDD form and transform to dataframes for building dataframes and fitting models.

The model in the original paper is Hierarchical Linear Model (HLM) which is an advanced form of linear regression that allows us to examine the effects of independent variables (diversity) on dependent variables (outcomes), taking into account potential correlations. They implement the analysis with lme4 package in R. But in Spark machine learning library, there is no model directly corresponding to HLM model. So, I choose the mostly related one in machine learning library in Spark -- generalized linear model (GLM) to achieve this project. We can set different features when analyzing the relation between a pair of variables. Then fit the model to our dataset. And based on the summary of generalized linear model, we can interpret the results and get conclusions. 


## II. Project Plan

### First week

- Determine project objectives and data mining goals. (20 hours)<br>

  Search and read related influential papers. Confirm project topic, analysis objectives and data source. These have been completed in the process of writing the bibliography.


- Collect initial data and explore data from data source. (5 hours)<br>

  Collect initial data from GHTorrent datasets on Google Bigquery. Based on the variables schema, list necessary datasets and variables. Make it clear about variable definitions and the corresponding relation structure between them.


- Select and filter data. (8 hours)<br>

  Select required variables from different datasets to create new tables. Then query distributions of key attributes and filter the data with several conditions. After multiple rounds of filtering, we can get effective data for analysis.


- Construct and integrate data. (8 hours)<br>

  Use filtered data to calculate the independene and response variables in this project. Union the column names in datasets. Export tables from Bigquery to Jupyter and download as .csv file to local.


### Second week

- Format data. (5 hours)<br>

  Upload tables of independent and response variables to Google drive. Import the data to Spark RDD form and transform to dataframes. 
  
  
- Select modelling technique. (3 hours)<br>

  Read papers to be familiar with the mechanism of HLM model. Get knowledges of the generalized linear model (GLM) functions. Verify if the data satisfies model assumptions.


- Build model. (8 hours)<br>

  Build generalized linear models among variables. Set features in different cases. Print the coefficient and intercept of each model. Print the summary of each model and show the significance of correlation among variables.


### Third week

- Evaluates result. (5 hours)<br>

  Use the model and its analysis result in the previous step to evaluate the relationship between independent and dependent variables. Interpret the results to get conclusions and explain the validation.


- Review process. (3 hours)<br>

  In this step, we have obtained appropriate model and conclusions. But it is also essential to do a more thorough review of the whole analysis process in order to determine if there is any important factor that has somehow been overlooked.


### Fourth week

- Produce final report. (10 hours)<br>

  Produce the final written report of the data analysis project. It includes all the previous deliverables, summary and the results.


- Review project. (2 hours)<br>

  Assess in the project implementation, what benefits and experience I get and what need to be improved in the further study as well.
  
## III. Data Understanding

### A. Basic Questions to Assess Structure

- Do all records in the dataset contain the same fields?

  The data source I use is GHTorrent data in google Bigquery. In a single dataset, all records have same fields. In different dataset, there will be other fields or same fields but different meaning. For the same field in a same dataset, it is possible that some rows (records) have empty value.
  
  
- How can you access the same fields across records?

  In google Bigquery, I can use SQL query directly to select same fields based on the field name across records in a same dataset. If there has same fields in several datasets, we need to check the schema to confirm the different definitions. Then we can select the fields from different positions.


- How are the record fields delimited from one another? Do you need to parse them?

  The record fields can be delimited by name which can show their meanings. The Bigquery will also provides schema to introduce field content and format. I do need to parse them for more details about the definition and relations so that I can select valid data for my project.

### B. Basic Questions to Assess Data Granularity.

- What kind of thing (person, object, relationship, event, etc.) do the records represent?

  With GHTorrent dataset in my project, there are three kinds of dataset, users, projects and events. In users dataset, each record represents a person with his id, name, country_code, email and some other personal information. In projects dataset, each record represents a project with id, language, created_at and so on. In events datasets like commits, pull_requests, issue_comments, each row is a responding event with id, event time, related user etc.


- Are the record homogeneous? Or heterogeneous?

  Users, projects and events dataset are heterogeneous recording different objects and fields. Some datasets like commits, pull_requests, commit_comments dataset are homogeneous. They record similar information related to an event like time, projectId, userId and content.


- What alternative interpretations of the records are there?

  It is possible to have alternative interpretations of records since there will be intersection between two datasets. Like project_member dataset has information about userId. This part of people are users and members as well.
  
### C. Basic Questions to Assess Data Accuracy.

- For data times, are time zones included or adjusted into a standard time zone like UTC? The format of times. Are the positions of month and day field ambiguous?

  In GHTorrent datasets I use in Bigquery, the time zones have been adjusted into a standard time zone UTC. The times are presented in 24-hour format. The positions of month and day field are not ambiguous. The standard format is like "2015-09-30 04:51:08.000 UTC". The year, month, day field are ordered from left to right and divided by "-".


- Is the data entered by people? If so, there might be a high incidence of misspelling and nonstandard abbreviations.

  Here take the country_code information for example to show the inaccuracy. The country_code is not entered by people. There should have a list of all countries for users to choose. But the key point that may lead inaccuracy is most users will not choose to fill this field. So when we calculate the country diversity of a group, empty code will make the result inaccurate.


- Does the distribution of inaccuracies affect a large number of records?

  I did queries about the inaccuracy in country_code. At the beginning for all users, only less than 10 percent people have non-empty information. This inaccuracy affects a large number of records. It is too low to calculate country diversity for groups. So I run other filter conditions first. After several rounds of filtering, it has grown to more than 30 percent for all remained users. And more than 10% projects have at least 75% members that have non-empty country_code. So we can select these projects to calculate more accurate country diversity.
  
### D. Basic Questions to Assess Data Temporality.

- When was the dataset collected?

  In google Bigquery, it provides GHTorrent data dumped on several days. I use the data dumped on 2017-05-01. It includes project, event, member records before 2017-05-01.


- Were all the records and record firlds collected at the same time?

  Records are not collected at the same time. Before the dump date, the dataset will update when there are new members, new projects and events. After the data is dumped, the dataset will be unchanged. In a specific dataset, the information in different fields are collected at the same time.


- Are the timestamps associated with collection of the data known and avaliable or as associated metadata?

  For GHTorrent data, the timestamps is avaliable and associated with collection of the data as a record field. For example, in projects dataset, field created_at is to record the time when the project is created; in commits dataset, field created_at represents the time when the event happens.
  
### E. Basic Questions to Assess Data Scope.

- Given the granularity of the dataset, what characteristics of the thing represented by the records are captured by the record fields? What characteristics are not captured?

  Given the granularity of dataset, the record fields can represent members, projects and events. From the GHTorrent schema, we can also realize some relations between two datasets. For example, the id attribute in commits dataset responds to the commit_id in commit_comments dataset. Some information is not captured by the record fields like gender of users, the update status of members in a projects.
  


- For the analysis that you want to perform, can you deduce or infer additional relevant characteristics from the ones that you have?

  From the ghtorrent dataset, we can obtain basic attributes of datasets. Then in later analysis, we can use prepared data to infer the independent and response variables in my project. For example, in order to calculate the tenure diversity of members in a project, we need tenure of each member and their mean value. Each member's tenure in a quarter is defined as the number of days from the first event to the end of the quarter. So we need to combine event tables and group by projectId and memberId to show all events from each member in each project. Then get tenure of each member to calculate the tenure diversity.


- Are there multiple records for the same thing? If so, does this change the granularity of your dataset or require some amount of deduplication before analysis?

  Multiple records can be likely to give more additional details or information about a same thing. For example, a user can be member of multiple projects and has many event records in different projects; several commit_comments may lead to a same commit. But in all datasets, each smallest unit objects like a commit_comment will have a unique id so I do not use some amount of deduplication.

## IV. Data Preparation

The most important work in this first week is select related datasets and prepare effective data for accurate analysis.Based on previous section, the data source of my project is the GHTorrent datasets on Google Bigquery. Bigquery stores similar GHTorrent datasets dumped on different dates. I use the data in ght_2017_05_01.

### A. Data Selection

The following related datasets and attributes from the GHTorrent schema will be needed in this project.

- projects: id, created_at

- users: id, country_code

- project_members: user_id, repo_id, created_at

- commits: id, committer_id, project_id, created_at

- commit_comments: id, commit_id, user_id, created_at

- pull_requests: id, base_repo_id, pullreq_id

- pull_request_history: id, created_at, actor_id, pull_request_id

- issues: id, issue_id, repo_id

- issue_events: issue_id, actor_id, event_id, created_at

- issue_comments: issue_id, comment_id, user_id, created_at

### B. Data Filter

Because of large size of initial datastes and convenient query and storage environment on Google Cloud Platform, I filter the selected initial data in Bigquery. Then save the results in new tables. I also create a project in mu account on Google Cloud Platform. Use pandas functions to read tables from Bigquery to Jupyter notebook. This part shows filter results and queries are in the comments.

Firstly, list filter conditions in preprosessing stage:

- Choose projects which were created before 2016-11-01.<br>

  Our data is dumped on 2017-05-01. Since the project group diversity will change as time goes on, we set a 90-day long period as a quarter and measure the diversity and memberwithdrawal in each quarter. So, we require projects which have at least 2 quarters (6 months).
  
  
- Choose projects whose memberCount >= 3.<br>

  The project topic is about group diversity. We define at least 3 members can compose a group.
  
   
- Choose groups in which at least 75% members have non-empty country_code.<br>

  Based on the reference, the country diversity is effective when we can get at least 75% members' country_code.
  

- Consider project events in recent one year (after 2016-05-01).<br>
  
  
- Filter inactive projects whose commitCount < 10.<br>
  
  Based on the reference, if the number of commits made by members is less than 10 or the total number of events is less than 100, the project is inactive. The filer condition about the total events will be implemented in later part.
  
#### a. Choose projects which were created before 2016-11-01.

``` sql
SELECT id, created_at
FROM [ghtorrent-bq:ght_2017_05_01.projects]
HAVING YEAR(created_at) < 2016 OR (YEAR(created_at) = 2016 AND MONTH(created_at) < 11)
ORDER BY created_at
```
#### b. Choose projects whose memberCount >= 3.

``` sql
--(1) Select all users who are project members and their countryCode and countryState (if the country_code is non-empty).

SELECT m.repo_id as projectId, u.id as memberId, u.country_code as countryCode,
  (CASE when u.country_code is null then 0
        when u.country_code is not null then 1
    END) as countryState
FROM [ghtorrent-bq:ght_2017_05_01.users] as u
INNER JOIN
(
  SELECT *
  FROM [ghtorrent-bq:ght_2017_05_01.project_members]
) as m
ON u.id = m.user_id

--(2) Combine the results in a and b(1) based on projectId to get members information in projects which are created before 2016-11-01 (table members1)

SELECT a.projectId as projectId, a.memberId as memberId, a.countryCode as countryCode, a.countryState as countryState
FROM
(
  SELECT m.repo_id as projectId, u.id as memberId, u.country_code as countryCode,
        (CASE when u.country_code is null then 0
              when u.country_code is not null then 1
         END) as countryState
  FROM [ghtorrent-bq:ght_2017_05_01.users] as u
  INNER JOIN
  (
    SELECT *
    FROM [ghtorrent-bq:ght_2017_05_01.project_members]
  ) as m
  ON u.id = m.user_id
) as a
INNER JOIN
(
  SELECT p.id as projectId
  FROM 
  (SELECT id, created_at
  FROM [ghtorrent-bq:ght_2017_05_01.projects]
  HAVING YEAR(created_at) < 2016 OR (YEAR(created_at) = 2016 AND MONTH(created_at) < 11)
  ) as p
) b
ON b.projectId = a.projectId

--(3) Count the number of members in each project and choose projects whose memberCount >= 3 (table projects1)
--Note: New tables are stored in a project in my google account. The project id is 'advance-topic-197921'.

SELECT projectId, count(memberId) as memberCount
FROM [advance-topic-197921:members.members1]
GROUP BY projectId
HAVING memberCount >= 3

--(4) Members in projects whose memberCount >= 3 (table members2)

SELECT m1.projectId as projectId, m1.memberId as memberId, m1.countryCode as countryCode, m1.countryState as countryState
FROM [advance-topic-197921:members.members1] as m1
INNER JOIN
(
  SELECT projectId
  FROM [advance-topic-197921:projects.projects1]
) as p1
ON p1.projectId = m1.projectId
ORDER BY m1.projectId
```
#### c. Choose groups in which at least 75% members have non-empty country_code

``` sql
-- (1) Calculate the percentage of members with non-empty country_code in each group.
SELECT projectId, SUM(countryState)/COUNT(countryState) as countryPercent
FROM [advance-topic-197921:members.members2]
GROUP BY projectId
ORDER BY countryPercent DESC

--(2) Choose projects in which at least 75% members have country_code (table projects2).
SELECT projectId, SUM(countryState)/COUNT(countryState) as countryPercent
FROM [advance-topic-197921:members.members2]
GROUP BY projectId
HAVING countryPercent >= 0.75
ORDER BY countryPercent DESC

--(3) Members in projects2 (table members3).

SELECT m2.projectId as projectId, m2.memberId as memberId, m2.countryCode as countryCode, m2.countryState as countryState
FROM [advance-topic-197921:members.members2] as m2
INNER JOIN
(
  SELECT projectId
  FROM [advance-topic-197921:projects.projects2]
) as p2
ON p2.projectId = m2.projectId
```
#### d. Filter inactive projects whose commitCount < 10.
```sql
--(1) Commits from member in member3, only consider events between 2016-05-01 and 2017-05-01(table commits1)

SELECT c.id as commitId, c.committer_id as committerId, c.project_id as projectId, c.created_at as createdAt
FROM
  (
    SELECT *
    FROM [ghtorrent-bq:ght_2017_05_01.commits] 
    HAVING (YEAR(created_at) = 2016 AND MONTH(created_at) >= 5) OR (YEAR(created_at) = 2017 AND MONTH(created_at) <= 5)
  ) as c
INNER JOIN
  (
    SELECT projectId, memberId
    FROM [advance-topic-197921:members.members3]
  ) as m3
 ON m3.projectId = c.project_id AND m3.memberId = c.committer_id

--(2) Calculate commitcount in each project(table commitcount)

SELECT projectId, COUNT(commitId) as commitCount
FROM [advance-topic-197921:commits.commits1]
GROUP BY projectId
ORDER BY commitCount

--(3) Projects with commitCount >= 10 on the basis of previous filter conditions (table projects3)

SELECT *
FROM [advance-topic-197921:commits.commitcount]
HAVING commitCount >= 10

--(4) Members in projects whose commitCount >= 10 (table members4)

SELECT m3.projectId as projectId, m3.memberId as memberId, m3.countryCode as countryCode, m3.countryState as countryState
FROM [advance-topic-197921:members.members3] as m3
INNER JOIN
(
  SELECT projectId
  FROM [advance-topic-197921:projects.projects3]
) as p3
ON p3.projectId = m3.projectId

--(5) Select commits made by members4 (commits2).

SELECT c.commitId as commitId, c.committerId as committerId, c.projectId as projectId, c.createdAt as createdAt
FROM [advance-topic-197921:commits.commits1] as c
INNER JOIN
(
  SELECT projectId, memberId
  FROM [advance-topic-197921:members.members4]
) as m4
ON m4.projectId = c.projectId AND m4.memberId = c.committerId
```
### C. Construct Data
During the filter process in last part, we have completed constructing new datasets including projects3, members4 and commits2. Then in this part, we will construct other event datasets based on id in project3 and members4. Then we can calculate the number of all events in each project. There will have another filter condition to exclude inactive projects whose total number of events is less than 100.

#### a. Commit_comments made by member4 between 2016-05-01 and 2017-05-01 (table commit_comments1)

Note: we will combine commits and commit_comments based on commit_id because the commit_comments need projectId attribute.

```sql
SELECT c.projectId as projectId, c.userId as memberId, c.commitCommentId as commitCommentId, c.createdAt as createdAt
FROM
(
SELECT c1.user_id as userId, c2.project_id as projectId, c1.id as commitCommentId, c1.created_at as createdAt
FROM [ghtorrent-bq:ght_2017_05_01.commit_comments] as c1
INNER JOIN
  (
    SELECT id, project_id
    FROM [ghtorrent-bq:ght_2017_05_01.commits]
  ) as c2
ON c1.commit_id = c2.id
) as c
INNER JOIN
(
  SELECT projectId, memberId
  FROM [advance-topic-197921:members.members4]
) as m4
ON m4.projectId = c.projectId AND m4.memberId = c.userId
HAVING (YEAR(createdAt) = 2016 AND MONTH(createdAt) >= 5) OR (YEAR(createdAt) = 2017 AND MONTH(createdAt) <= 5)
```
#### b. Pull-request events between 2016-05-01 and 2017-05-01 from members4 (table pull_requests1)

Note: we need combine pull_requests and pull_request_history based on pull_request_id because one pull_request may have several related events.

```sql
SELECT p.pullRequestId as pullRequestId, p.repoId as repoId, p.pullRequestEventId as pullRequestEventId, 
      p.eventCreatedAt as eventCreatedAt, p.eventActorId as eventActorId
FROM
(
SELECT pr.pullreq_id as pullRequestId, pr.base_repo_id as repoId, ph.id as pullRequestEventId, 
      ph.created_at as eventCreatedAt, ph.actor_id as eventActorId
FROM [ghtorrent-bq:ght_2017_05_01.pull_request_history] as ph
INNER JOIN
(
  SELECT id, base_repo_id, pullreq_id
  FROM [ghtorrent-bq:ght_2017_05_01.pull_requests]
) as pr
ON ph.pull_request_id = pr.id
HAVING (YEAR(eventCreatedAt) = 2016 AND MONTH(eventCreatedAt) >= 5) OR (YEAR(eventCreatedAt) = 2017 AND MONTH(eventCreatedAt) <= 5)
) as p
INNER JOIN
(
  SELECT projectId, memberId
  FROM [advance-topic-197921:members.members4]
) as m4
ON m4.projectId = p.repoId AND m4.memberId = p.eventActorId
```
#### c. Issue events between 2016-05-01 and 2017-05-01 from members4 (table issues1).

```sql
SELECT i.issueId as issueId, i.repoId as repoId, i.actorId as actorId, i.issueEventId as issueEventId, i.createdAt as createdAt
FROM
(
SELECT i.issue_id as issueId, i.repo_id as repoId, ie.actor_id as actorId, ie.event_id as issueEventId, ie.created_at as createdAt
FROM [ghtorrent-bq:ght_2017_05_01.issue_events] as ie
INNER JOIN
(
  SELECT id, issue_id, repo_id
  FROM [ghtorrent-bq:ght_2017_05_01.issues]
) as i
ON ie.issue_id = i.id
HAVING (YEAR(CreatedAt) = 2016 AND MONTH(CreatedAt) >= 5) OR (YEAR(CreatedAt) = 2017 AND MONTH(CreatedAt) <= 5)
) as i
INNER JOIN
(
  SELECT projectId, memberId
  FROM [advance-topic-197921:members.members4]
) as m4
ON m4.projectId = i.repoId AND m4.memberId = i.actorId
```
#### d. Issue_comments between 2016-05-01 and 2017-05-01 from members4 (table issue_comments1)

```sql
SELECT i.issueId as issueId, i.repoId as repoId, i.userId as userId, 
      i.issueCommentId as issueCommentId, i.issueCommentCreatedAt as issueCommentCreatedAt
FROM
(
SELECT i.issue_id as issueId, i.repo_id as repoId, ic.user_id as userId,
      ic.comment_id as issueCommentId, ic.created_at as issueCommentCreatedAt
FROM [ghtorrent-bq:ght_2017_05_01.issue_comments] as ic
INNER JOIN
(
  SELECT id, issue_id, repo_id
  FROM [ghtorrent-bq:ght_2017_05_01.issues]
) as i
ON i.id = ic.issue_id
HAVING (YEAR(issueCommentCreatedAt) = 2016 AND MONTH(issueCommentCreatedAt) >= 5) OR 
      (YEAR(issueCommentCreatedAt) = 2017 AND MONTH(issueCommentCreatedAt) <= 5)
) as i
INNER JOIN
(
  SELECT projectId, memberId
  FROM [advance-topic-197921:members.members4]
) as m4
ON m4.projectId = i.repoId AND m4.memberId = i.userId
```

#### e. Calculate numbers of each kind of events.

```sql
--(1) Calculate commitCommentCount

SELECT projectId, COUNT(commitCommentId) as commitCommentCount
FROM [advance-topic-197921:commit_comments.commit_comments1]
GROUP BY projectId
ORDER BY commitCommentCount

--(2) Calculate issueCommentCount

SELECT repoId, COUNT(issueCommentId) as issueCommentCount
FROM [advance-topic-197921:issue_comments.issue_comments1]
GROUP BY repoId
ORDER BY issueCommentCount

--(3) Calculate issueEventCount

SELECT repoId, COUNT(issueEventId) as issueEventCount
FROM [advance-topic-197921:issues.issues1]
GROUP BY repoId
ORDER BY issueEventCount

--(4) Calculate pullrequestEventCount

SELECT repoId, COUNT(pullRequestEventId) as pullrequestEventCount
FROM [advance-topic-197921:pull_requests.pull_requests1]
GROUP BY repoId
ORDER BY pullrequestEventCount
```
### D. Integrate Data

In order to calculate our dependent variables (tenure and country diversity) and independent variables (group productivity and member withdrawal), it is important to integrate tables above.</br>

In this part, we also use the filter condition to exclude inactive projects whose total number of events is less than 100. Then we can get final tables about projects, members and events. I use pandas functions to download then from Bigquery to local. Then upload the data to google drive and import to Spark.

#### a. Show the number of all events in each project in table 'projects' (project_events1).

Based on the reference paper, projects with total number of events less than 100 are inactive. I also do this query on Bigquery and combine tables 'projects3', 'commitCommentCount', 'issueCommentCount', 'issueEventCount', 'pullrequestEventCount'.

```sql
SELECT e3.projectId as projectId, e3.commitCount as commitCount, e3.commitCommentCount as commitCommentCount,
      e3.issueCommentCount as issueCommentCount, e3.issueEventCount as issueEventCount, pr.pullrequestEventCount as    pullrequestEventCount
FROM
(
SELECT e2.projectId as projectId, e2.commitCount as commitCount, e2.commitCommentCount as commitCommentCount,
      e2.issueCommentCount as issueCommentCount, ie.issueEventCount as issueEventCount
FROM
(
SELECT e1.projectId as projectId, e1.commitCount as commitCount, e1.commitCommentCount as commitCommentCount,
      ic.issueCommentCount as issueCommentCount
FROM
(
SELECT p3.projectId as projectId, p3.commitCount as commitCount, cc.commitCommentCount as commitCommentCount     
FROM [advance-topic-197921:projects.projects3] as p3
LEFT JOIN
(
  SELECT projectId, commitCommentCount
  FROM [advance-topic-197921:commit_comments.commitCommentCount]
) as cc
ON p3.projectId = cc.projectId
) as e1
LEFT JOIN
(
  SELECT repoId, issueCommentCount
  FROM [advance-topic-197921:issue_comments.issueCommentCount]
) as ic
ON e1.projectId = ic.repoId
) as e2
LEFT JOIN
(
  SELECT repoId, issueEventCount
  FROM [advance-topic-197921:issues.issueEventCount]
) as ie
ON e2.projectId = ie.repoId
) as e3
LEFT JOIN
(
  SELECT repoId, pullrequestEventCount
  FROM [advance-topic-197921:pull_requests.pullrequestEventCount]
) as pr
ON e3.projectId = pr.repoId

--Calculate the sum of events in each projects and choose the number >= 100

SELECT projectId, (commitCount+commitCommentCount+issueCommentCount+issueEventCount+pullrequestEventCount) as projectEventSum
FROM [advance-topic-197921:project_events.project_events1]
HAVING projectEventSum >= 100
ORDER BY projectEventSum
```
#### b. Now we get new projectId list. We can get new members table (members5).
```sql
SELECT m4.projectId as projectId, m4.memberId as memberId, m4.countryCode as countryCode, m4.countryState as countryState
FROM [advance-topic-197921:project_events.projectEventSum] pe
INNER JOIN
(
  SELECT projectId, memberId, countryCode, countryState
  FROM [advance-topic-197921:members.members4]
) as m4
ON pe.projectId = m4.projectId
```
#### c. Combine the members5 with project created_at information to get final members table (members6, 3131 rows)

For next final tables, use pandas functions to download them to local. Then upload the data to google drive and import to spark.

```sql
SELECT m5.projectId as projectId, p.created_at as projectCreatedAt, m5.memberId as memberId,
      m5.countryCode as countryCode, m5.countryState as countryState
FROM [advance-topic-197921:members.members5] as m5
INNER JOIN
(
   SELECT id, created_at
   FROM [ghtorrent-bq:ght_2017_05_01.projects]
 ) as p
ON p.id = m5.projectId
```
```python
df = pd.read_gbq('select * from members.members6', project_id='advance-topic-197921', index_col=None, col_order=None, reauth=False, verbose=True, 
                 private_key='', dialect='legacy')
df.to_csv('members6.csv', index = False, encoding = 'utf-8')
```
```spark
import java.sql.Timestamp
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

val members6DataURL = "https://drive.google.com/uc?export=download&id=1XXeEMpo0w3yQN4MJ0TTvzC1Q82Jhn9hO"
val members6RDD = sc.parallelize(IOUtils.toString(new URL(members6DataURL),Charset.forName("utf8")).split("\n")).
                                map(line => line.split(",", -1).map(_.trim)).filter(line => line(0) != "projectId")
```
```spark
members6RDD.take(5)
```
```spark
// members6 class
case class Members6(                            // column index
    projectId: Int,                             // 0
    projectCreatedAt: java.sql.Timestamp,       // 1
    memberId: Int,                              // 2  
    countryCode: String,                        // 3 
    countryState: Int                           // 4 
  
)

// patching the String class with new functions that have a defualt value if conversion to another type fails.
implicit class StringConversion(val s: String) {
def toTypeOrElse[T](convert: String=>T, defaultVal: T) = try {
    convert(s)
  } catch {
    case _: Throwable => defaultVal
  }
  
  def toIntOrElse(defaultVal: Int = 0) = toTypeOrElse[Int](_.toInt, defaultVal)
  def toDoubleOrElse(defaultVal: Double = 0D) = toTypeOrElse[Double](_.toDouble, defaultVal)
  def toDateOrElse(defaultVal: java.sql.Timestamp = java.sql.Timestamp.valueOf("1970-01-01 00:00:00")) = toTypeOrElse[java.sql.Timestamp](java.sql.Timestamp.valueOf(_), defaultVal)
}

// clean up fields and convert them to proper formats
def getMembers6Cleaned(row:Array[String]):Members6 = {
  return Members6(
    row(0).toIntOrElse(),
    row(1).toDateOrElse(),
    row(2).toIntOrElse(),
    row(3),
    row(4).toIntOrElse()
  )
}
```
```spark
// load the data into a DataFrame used for SparkSQL
val members6 = members6RDD.map(r => getMembers6Cleaned(r)).toDF()

// register this data as an SQL table
members6.createOrReplaceTempView("members6")
```
```sql
SELECT *
FROM members6
ORDER BY projectId
LIMIT 10
```
