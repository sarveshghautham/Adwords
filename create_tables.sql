create table Queries (
qid INTEGER, 
query VARCHAR2(400)
);

create table Advertisers
(
advertiserId INTEGER, 
budget FLOAT, 
ctc FLOAT
);

create table Keywords (
advertiserId INTEGER, 
keyword VARCHAR2(100), 
bid FLOAT
);

create table temp_output (
qid INTEGER,
advertiserId INTEGER,
rank1 FLOAT,
rank2 FLOAT,
rank3 FLOAT,
bid FLOAT,
quality_score FLOAT
);

create table adv_finance (
adv_id INTEGER,
balance FLOAT
);

create table temp_adv (
adv_id INTEGER,
bid FLOAT
);

create table temp_bid (
bid FLOAT
);

create table ads_hits (
adv_id INTEGER,
hits INTEGER
);

create table temp_table (
keyword VARCHAR2(100),
query_count INTEGER,
advertiser_count INTEGER
);

create table task1_output (
qid INTEGER,
rank INTEGER,
advertiserId INTEGER,
balance FLOAT,
budget FLOAT
);

create table task2_output (
qid INTEGER,
rank INTEGER,
advertiserId INTEGER,
balance FLOAT,
budget FLOAT
);

create table task3_output (
qid INTEGER,
rank INTEGER,
advertiserId INTEGER,
balance FLOAT,
budget FLOAT
);

create table task4_output (
qid INTEGER,
rank INTEGER,
advertiserId INTEGER,
balance FLOAT,
budget FLOAT
);

create table task5_output (
qid INTEGER,
rank INTEGER,
advertiserId INTEGER,
balance FLOAT,
budget FLOAT
);

create table task6_output (
qid INTEGER,
rank INTEGER,
advertiserId INTEGER,
balance FLOAT,
budget FLOAT
);

exit;
