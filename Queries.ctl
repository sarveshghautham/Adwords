LOAD DATA
INFILE Queries.dat
INTO TABLE Queries
REPLACE
FIELDS TERMINATED BY '	'
TRAILING NULLCOLS
(
QID,
QUERY
)
