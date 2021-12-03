drop table if exists Releases;
drop table if exists Commits_Parent_Child;
drop table if exists Branches;
drop table if exists Snapshots;
drop table if exists Commits;
drop table if exists Repos;

CREATE TABLE Repos (
	Repo_Name varchar(100) NOT NULL,
	CONSTRAINT PK_Repos PRIMARY KEY CLUSTERED (Repo_Name)
);

insert into Repos(Repo_Name) values ('mpo-ui');

CREATE TABLE Snapshots (
	Repo_Name varchar(100) NOT NULL,
	Snapshot_Id bigint IDENTITY(1,1) NOT NULL,
	Snapshot_Timestamp smalldatetime NOT NULL,
	CONSTRAINT PK_Snapshots PRIMARY KEY CLUSTERED (Repo_Name, Snapshot_Id),
	CONSTRAINT FK_Snapshots_Repo_Name FOREIGN KEY (Repo_Name) REFERENCES Repos(Repo_Name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Commits (
	Repo_Name varchar(100) NOT NULL,
	Commit_Hash varchar(100) NOT NULL,
	Commit_Timestamp smalldatetime NOT NULL,
	Release_Timestamp smalldatetime NULL,
	CONSTRAINT PK_Commits PRIMARY KEY CLUSTERED (Repo_Name, Commit_Hash)
);

CREATE TABLE Commits_Parent_Child (
	Repo_Name varchar(100) NOT NULL,
	Commit_Child_Hash varchar(100) NOT NULL,
	Commit_Parent_Hash varchar(100) NOT NULL,
	CONSTRAINT PK_Commits_Parent_Child PRIMARY KEY CLUSTERED (Repo_Name, Commit_Child_Hash, Commit_Parent_Hash)
	--CONSTRAINT FK_Commits_Child FOREIGN KEY (Repo_Name, Commit_Child_Hash) REFERENCES Commits(Repo_Name, Commit_Hash),
	--CONSTRAINT FK_Commits_Parent FOREIGN KEY (Repo_Name, Commit_Parent_Hash) REFERENCES Commits(Repo_Name, Commit_Hash)
);

CREATE TABLE Branches (
	Repo_Name varchar(100) NOT NULL,
	Snapshot_Id bigint NOT NULL,
	Branch_Name varchar(100) NOT NULL,
	Commit_Hash varchar(100) NOT NULL,
	CONSTRAINT PK_Branches PRIMARY KEY CLUSTERED (Repo_Name, Snapshot_Id, Branch_Name),
	 CONSTRAINT FK_Branches_Snapshot_Id FOREIGN KEY (Repo_Name, Snapshot_Id) REFERENCES Snapshots(Repo_Name, Snapshot_Id)
	-- CONSTRAINT FK_Branches_Commit_Hash FOREIGN KEY (Repo_Name, Commit_Hash) REFERENCES Commits(Repo_Name, Commit_Hash)
);


CREATE TABLE Releases (
	Repo_Name varchar(100) NOT NULL,
	Release_Name varchar(100) NOT NULL,
	Release_Timestamp smalldatetime NOT NULL,
	Commit_Hash varchar(100) NOT NULL,
	CONSTRAINT PK_Tags PRIMARY KEY CLUSTERED (Repo_Name, Release_Name)
	-- CONSTRAINT FK_Releases_Commit_Hash FOREIGN KEY (Repo_Name, Commit_Hash) REFERENCES Commits(Repo_Name, Commit_Hash)
);

select count(*) as "Active Branch Count"
from Branches as b
inner join Commits as c on c.Commit_Hash = b.Commit_Hash
where
1=1
and c.Repo_Name = ''
and c.Release_Timestamp is not null
and Snapshot_Id = 1;

select count(*) as "Inactive Branch Count"
  from Branches as b
inner join Commits as c on c.Commit_Hash = b.Commit_Hash
where
1=1
and c.Repo_Name = ''
and c.Release_Timestamp is null
and Snapshot_Id = 1;

select count(*) as "Unreleased Commit Count"
from Commits c
where
1=1
and c.Release_Timestamp is null
and c.Repo_Name = '';

select month(c.Release_Timestamp) as 'Month', count(*) as 'Commit Count', avg(datediff(day, c.Commit_Timestamp, c.Release_Timestamp)) as "Average Lead Time"
from Commits c
where
1=1
and c.Repo_Name = ''
and c.Release_Timestamp is not null
group by month(c.Release_Timestamp);

SELECT DATEADD(YEAR, -1, MONTH(GETDATE()));
select DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1);


WITH Months AS
(
     SELECT DATEADD(YEAR, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)) AS Dates
  
     UNION ALL
  
     SELECT DATEADD(MONTH, 1, Dates)
     FROM Months
     WHERE CONVERT(DATE, Dates) < CONVERT(DATE, GETDATE())
),
ReleasedCommits As
(
	select *
	from Commits c
	where
	1=1
	and c.Release_Timestamp is not null
	and c.Repo_Name = ''
)
SELECT DATENAME(MONTH, Dates) + ' ' + DATENAME(YEAR, Dates), (
	select --avg(DATEDIFF(day, c.Commit_Timestamp, c.Release_Timestamp)),
		count(*)
	from ReleasedCommits c
	where year(c.Release_Timestamp) = year(m.Dates) and month(c.Release_Timestamp) = month(m.Dates)
)
FROM Months m --left join Commits c on year(c.Release_Timestamp) = year(m.Dates) and month(c.Release_Timestamp) = month(m.Dates)
