LOAD DATA
INFILE Advertisers.dat
INTO TABLE Advertisers
REPLACE
FIELDS TERMINATED BY '	'
TRAILING NULLCOLS
(
advertiserId,
budget,
ctc
)
