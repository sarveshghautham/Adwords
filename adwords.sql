CREATE OR REPLACE PROCEDURE update_balance_algo_rank(advertiser_id NUMBER, balance FLOAT)
AS
	ad_rank FLOAT;
	
	cursor c(adv_id NUMBER) is
		SELECT qid, quality_score
		FROM temp_output
		WHERE advertiserId = adv_id;

BEGIN
	FOR each_row in c(advertiser_id)
	LOOP
		ad_rank := balance * each_row.quality_score;
		UPDATE temp_output
		SET rank2 = ad_rank
		WHERE advertiserId = advertiser_id AND
		qid = each_row.qid;
	END LOOP;

END update_balance_algo_rank;
/
CREATE OR REPLACE PROCEDURE update_g_balance_algo_rank (advertiser_id NUMBER, balance FLOAT)
AS
	ad_rank FLOAT;
	phi FLOAT;
	budget_fraction FLOAT;
	adv_budget FLOAT;
	e FLOAT;
	
	cursor c(adv_id NUMBER) is
		SELECT qid, quality_score, bid
		FROM temp_output
		WHERE advertiserId = adv_id;
BEGIN
	e := 2.71828;
	
	SELECT budget INTO adv_budget
	FROM Advertisers
	WHERE advertiserId = advertiser_id;

	budget_fraction := balance/adv_budget;
	FOR each_row in c(advertiser_id)
	LOOP
		phi := each_row.bid * (1 - (POWER(e,-budget_fraction)));
		ad_rank := phi * each_row.quality_score;
		
		UPDATE temp_output
		SET rank3 = ad_rank
		WHERE qid = each_row.qid AND
		advertiserId = advertiser_id;
	END LOOP;
END update_g_balance_algo_rank;
/
CREATE OR REPLACE PROCEDURE construct_finance_table
AS
	cursor c is
		SELECT advertiserId, budget
		FROM Advertisers;

BEGIN
	FOR each_row in c
	LOOP
		INSERT INTO adv_finance VALUES (each_row.advertiserId, each_row.budget);
	END LOOP;
	COMMIT;
END construct_finance_table;
/
CREATE OR REPLACE PROCEDURE clear_tables
AS
BEGIN
	DELETE FROM adv_finance;
	DELETE FROM ads_hits;
END clear_tables;
/
CREATE OR REPLACE PROCEDURE first_price_auction_task1 (k NUMBER)
AS
	rank NUMBER;
	temp_count NUMBER;
	ad_hits NUMBER;
	adv_ctc NUMBER;
	bid_amount FLOAT;
	adv_balance FLOAT;
	adv_budget FLOAT;
	returned_rows NUMBER;
	count_adv NUMBER;

	cursor c is
		SELECT DISTINCT qid
		FROM temp_output
		ORDER BY qid ASC;
	cursor c1(query_id NUMBER) is
		SELECT *
		FROM (SELECT * 
		FROM temp_output
		ORDER BY rank1 DESC, advertiserId ASC)
		WHERE qid = query_id;
BEGIN
	construct_finance_table();
	FOR each_c_row in c
	LOOP
		SELECT COUNT(*) INTO returned_rows
              	FROM (SELECT *
               		FROM temp_output
               		ORDER BY rank1 DESC, advertiserId ASC)
               		WHERE qid = each_c_row.qid;

		if returned_rows >= k then
			returned_rows := k;
		end if;		

		count_adv := 0;

		rank := 1;
		FOR each_c1_row in c1(each_c_row.qid)
		LOOP	
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_c1_row.advertiserId;

			if adv_balance >= each_c1_row.bid then
		
				SELECT COUNT(*) INTO temp_count
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
		
				if temp_count = 0 then
					INSERT INTO ads_hits VALUES (each_c1_row.advertiserId, 1);
				else
					UPDATE ads_hits
					SET hits = hits+1
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT hits INTO ad_hits
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
	
				SELECT ctc INTO adv_ctc
				FROM advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				adv_ctc := adv_ctc * 100;

				if ad_hits = 100 then
					UPDATE ads_hits
					SET hits = 0
					WHERE adv_id = each_c1_row.advertiserId;
				end if;
		
				if ad_hits > 0 AND ad_hits <= adv_ctc then
					adv_balance := adv_balance - each_c1_row.bid;

					UPDATE adv_finance
					SET balance = adv_balance
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT budget INTO adv_budget
				FROM Advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				INSERT INTO task1_output VALUES (each_c_row.qid, rank, each_c1_row.advertiserId, adv_balance, adv_budget);
				rank := rank + 1;
				
				count_adv := count_adv + 1;
			end if;
			EXIT WHEN count_adv >= returned_rows;
		END LOOP;
	END LOOP;
	clear_tables();
END first_price_auction_task1;
/
CREATE OR REPLACE PROCEDURE second_price_auction_task2(k NUMBER)
AS
	rank NUMBER;
	temp_count NUMBER;
	ad_hits NUMBER;
	adv_ctc NUMBER;
	bid_amount FLOAT;
	adv_balance FLOAT;
	adv_budget FLOAT;
	returned_rows NUMBER;
	count_adv NUMBER;
	count_bidders NUMBER;
	second_bid FLOAT;

	cursor c is
		SELECT DISTINCT qid
		FROM temp_output
		ORDER BY qid ASC;
	cursor c1(query_id NUMBER) is
		SELECT *
		FROM (SELECT * 
		FROM temp_output
		ORDER BY rank1 DESC, advertiserId ASC)
		WHERE qid = query_id;
BEGIN
	construct_finance_table();	
	FOR each_c_row in c
	LOOP
		SELECT COUNT(*) INTO returned_rows
              	FROM (SELECT *
               		FROM temp_output
               		ORDER BY rank1 DESC, advertiserId ASC)
               		WHERE qid = each_c_row.qid;

		if returned_rows >= k then
			returned_rows := k;
		end if;		

		count_adv := 0;
		rank := 1;

		FOR each_bid_row in c1(each_c_row.qid)
		LOOP
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_bid_row.advertiserId;
			
			--dbms_output.put_line('adv_balance: '||adv_balance);
			--dbms_output.put_line('bid: '||each_bid_row.bid);
			if adv_balance >= each_bid_row.bid then 
				--dbms_output.put_line('inserting bid: '||each_bid_row.bid);
				INSERT INTO temp_bid VALUES (each_bid_row.bid);
			end if;
		END LOOP;

		FOR each_c1_row in c1(each_c_row.qid)
		LOOP	
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_c1_row.advertiserId;

			if adv_balance >= each_c1_row.bid then
		
				SELECT COUNT(*) INTO temp_count
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
		
				if temp_count = 0 then
					INSERT INTO ads_hits VALUES (each_c1_row.advertiserId, 1);
				else
					UPDATE ads_hits
					SET hits = hits+1
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT hits INTO ad_hits
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
	
				SELECT ctc INTO adv_ctc
				FROM advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				adv_ctc := adv_ctc * 100;

				if ad_hits = 100 then
					UPDATE ads_hits
					SET hits = 0
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT count(*) INTO count_bidders
				FROM temp_output
				WHERE bid < each_c1_row.bid
				AND qid = each_c_row.qid;

				if count_bidders = 0 then
					second_bid := each_c1_row.bid;
				else
					SELECT MAX(bid) INTO second_bid
					FROM temp_bid
					WHERE bid < each_c1_row.bid;
				end if;
			
				--	dbms_output.put_line ('my bid: '||each_c1_row.bid);
				--	dbms_output.put_line ('second bid: '||second_bid);
				if ad_hits > 0 AND ad_hits <= adv_ctc then
					adv_balance := adv_balance - second_bid;

					UPDATE adv_finance
					SET balance = adv_balance
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT budget INTO adv_budget
				FROM Advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				INSERT INTO task2_output VALUES (each_c_row.qid, rank, each_c1_row.advertiserId, adv_balance, adv_budget);
				rank := rank + 1;
				
				count_adv := count_adv + 1;
			end if;
			EXIT WHEN count_adv >= returned_rows;
		END LOOP;
		DELETE FROM temp_bid;
	END LOOP;
	clear_tables();
END second_price_auction_task2;
/
CREATE OR REPLACE PROCEDURE first_price_auction_task3(k NUMBER)
AS
	rank NUMBER;
	temp_count NUMBER;
	ad_hits NUMBER;
	adv_ctc NUMBER;
	bid_amount FLOAT;
	adv_balance FLOAT;
	adv_budget FLOAT;
	returned_rows NUMBER;
	count_adv NUMBER;
	ad_rank FLOAT;

	cursor c is
		SELECT DISTINCT qid
		FROM temp_output
		ORDER BY qid ASC;
	cursor c1(query_id NUMBER) is
		SELECT * 
		FROM temp_output
		WHERE qid = query_id
		ORDER BY rank2 DESC, advertiserId ASC;
BEGIN
	construct_finance_table();
	calculate_rank_task3();	
	FOR each_c_row in c
	LOOP

		SELECT COUNT(*) INTO returned_rows
                FROM temp_output
                WHERE qid = each_c_row.qid
               	ORDER BY rank2 DESC, advertiserId ASC;

		if returned_rows >= k then
			returned_rows := k;
		end if;		

		count_adv := 0;

		rank := 1;
		FOR each_c1_row in c1(each_c_row.qid)
		LOOP	
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_c1_row.advertiserId;

			if adv_balance >= each_c1_row.bid then
		
				SELECT COUNT(*) INTO temp_count
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
		
				if temp_count = 0 then
					INSERT INTO ads_hits VALUES (each_c1_row.advertiserId, 1);
				else
					UPDATE ads_hits
					SET hits = hits+1
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT hits INTO ad_hits
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
	
				SELECT ctc INTO adv_ctc
				FROM advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				adv_ctc := adv_ctc * 100;

				if ad_hits = 100 then
					UPDATE ads_hits
					SET hits = 0
					WHERE adv_id = each_c1_row.advertiserId;
				end if;
		
				if ad_hits > 0 AND ad_hits <= adv_ctc then
					adv_balance := adv_balance - each_c1_row.bid;

					UPDATE adv_finance
					SET balance = adv_balance
					WHERE adv_id = each_c1_row.advertiserId;

					update_balance_algo_rank(each_c1_row.advertiserId, adv_balance);
				end if;

				SELECT budget INTO adv_budget
				FROM Advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				INSERT INTO task3_output VALUES (each_c_row.qid, rank, each_c1_row.advertiserId, adv_balance, adv_budget);
				rank := rank + 1;
				
				count_adv := count_adv + 1;
			end if;
			EXIT WHEN count_adv >= returned_rows;
		END LOOP;
	END LOOP;
	clear_tables;
END first_price_auction_task3;
/
CREATE OR REPLACE PROCEDURE second_price_auction_task4(k NUMBER)
AS
	rank NUMBER;
	temp_count NUMBER;
	ad_hits NUMBER;
	adv_ctc NUMBER;
	bid_amount FLOAT;
	adv_balance FLOAT;
	adv_budget FLOAT;
	returned_rows NUMBER;
	count_adv NUMBER;
	ad_rank FLOAT;
	count_bidders NUMBER;
	second_bid FLOAT;

	cursor c is
		SELECT DISTINCT qid
		FROM temp_output
		ORDER BY qid ASC;
	cursor c1(query_id NUMBER) is
		SELECT * 
		FROM temp_output
		WHERE qid = query_id
		ORDER BY rank2 DESC, advertiserId ASC;
BEGIN
	construct_finance_table();
	calculate_rank_task3();	
	FOR each_c_row in c
	LOOP
		SELECT COUNT(*) INTO returned_rows
                FROM temp_output
                WHERE qid = each_c_row.qid
               	ORDER BY rank2 DESC, advertiserId ASC;

		if returned_rows >= k then
			returned_rows := k;
		end if;		

		count_adv := 0;

		rank := 1;
		FOR each_bid_row in c1(each_c_row.qid)
		LOOP
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_bid_row.advertiserId;

			if adv_balance >= each_bid_row.bid then 
				INSERT INTO temp_bid VALUES (each_bid_row.bid);
			end if;
		END LOOP;
		FOR each_c1_row in c1(each_c_row.qid)
		LOOP	
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_c1_row.advertiserId;

			if adv_balance >= each_c1_row.bid then
		
				SELECT COUNT(*) INTO temp_count
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
		
				if temp_count = 0 then
					INSERT INTO ads_hits VALUES (each_c1_row.advertiserId, 1);
				else
					UPDATE ads_hits
					SET hits = hits+1
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT hits INTO ad_hits
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
	
				SELECT ctc INTO adv_ctc
				FROM advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				adv_ctc := adv_ctc * 100;

				if ad_hits = 100 then
					UPDATE ads_hits
					SET hits = 0
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT count(*) INTO count_bidders
				FROM temp_output
				WHERE bid < each_c1_row.bid
				AND qid = each_c_row.qid;

				if count_bidders = 0 then
					second_bid := each_c1_row.bid;
				else
					SELECT MAX(bid) INTO second_bid
					FROM temp_bid
					WHERE bid < each_c1_row.bid;
				end if;
		
				if ad_hits > 0 AND ad_hits <= adv_ctc then
					adv_balance := adv_balance - second_bid;

					UPDATE adv_finance
					SET balance = adv_balance
					WHERE adv_id = each_c1_row.advertiserId;

					update_balance_algo_rank(each_c1_row.advertiserId, adv_balance);
				end if;

				SELECT budget INTO adv_budget
				FROM Advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				INSERT INTO task4_output VALUES (each_c_row.qid, rank, each_c1_row.advertiserId, adv_balance, adv_budget);
				rank := rank + 1;
				
				count_adv := count_adv + 1;
			end if;
			EXIT WHEN count_adv >= returned_rows;
		END LOOP;
		DELETE FROM temp_bid;
	END LOOP;
	clear_tables();
END second_price_auction_task4;
/
CREATE OR REPLACE PROCEDURE first_price_auction_task5(k NUMBER)
AS
	rank NUMBER;
	temp_count NUMBER;
	ad_hits NUMBER;
	adv_ctc NUMBER;
	bid_amount FLOAT;
	adv_balance FLOAT;
	adv_budget FLOAT;
	returned_rows NUMBER;
	count_adv NUMBER;

	cursor c is
		SELECT DISTINCT qid
		FROM temp_output
		ORDER BY qid ASC;
	cursor c1(query_id NUMBER) is
		SELECT *
		FROM (SELECT * 
		FROM temp_output
		ORDER BY rank3 DESC, advertiserId ASC)
		WHERE qid = query_id;
BEGIN
	construct_finance_table();
	calculate_rank_task5();	
	FOR each_c_row in c
	LOOP
		SELECT COUNT(*) INTO returned_rows
              	FROM (SELECT *
               		FROM temp_output
               		ORDER BY rank3 DESC, advertiserId ASC)
               		WHERE qid = each_c_row.qid;

		if returned_rows >= k then
			returned_rows := k;
		end if;		

		count_adv := 0;

		rank := 1;
		FOR each_c1_row in c1(each_c_row.qid)
		LOOP	
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_c1_row.advertiserId;

			if adv_balance >= each_c1_row.bid then
		
				SELECT COUNT(*) INTO temp_count
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
		
				if temp_count = 0 then
					INSERT INTO ads_hits VALUES (each_c1_row.advertiserId, 1);
				else
					UPDATE ads_hits
					SET hits = hits+1
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT hits INTO ad_hits
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
	
				SELECT ctc INTO adv_ctc
				FROM advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				adv_ctc := adv_ctc * 100;

				if ad_hits = 100 then
					UPDATE ads_hits
					SET hits = 0
					WHERE adv_id = each_c1_row.advertiserId;
				end if;
		
				if ad_hits > 0 AND ad_hits <= adv_ctc then
					adv_balance := adv_balance - each_c1_row.bid;

					UPDATE adv_finance
					SET balance = adv_balance
					WHERE adv_id = each_c1_row.advertiserId;

					update_g_balance_algo_rank(each_c1_row.advertiserId, adv_balance);
				end if;

				SELECT budget INTO adv_budget
				FROM Advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				INSERT INTO task5_output VALUES (each_c_row.qid, rank, each_c1_row.advertiserId, adv_balance, adv_budget);
				rank := rank + 1;

				count_adv := count_adv + 1;
			end if;
			EXIT WHEN count_adv >= returned_rows;
		END LOOP;
	END LOOP;
	clear_tables();
END first_price_auction_task5;
/
CREATE OR REPLACE PROCEDURE second_price_auction_task6(k NUMBER)
AS
	rank NUMBER;
	temp_count NUMBER;
	ad_hits NUMBER;
	adv_ctc NUMBER;
	bid_amount FLOAT;
	adv_balance FLOAT;
	adv_budget FLOAT;
	returned_rows NUMBER;
	count_adv NUMBER;
	count_bidders NUMBER;
	second_bid FLOAT;

	cursor c is
		SELECT DISTINCT qid
		FROM temp_output
		ORDER BY qid ASC;
	cursor c1(query_id NUMBER) is
		SELECT *
		FROM (SELECT * 
		FROM temp_output
		ORDER BY rank3 DESC, advertiserId ASC)
		WHERE qid = query_id;
BEGIN
	construct_finance_table();
	calculate_rank_task5();	
	FOR each_c_row in c
	LOOP
		SELECT COUNT(*) INTO returned_rows
              	FROM (SELECT *
               		FROM temp_output
               		ORDER BY rank3 DESC, advertiserId ASC)
               		WHERE qid = each_c_row.qid;

		if returned_rows >= k then
			returned_rows := k;
		end if;		

		count_adv := 0;

		rank := 1;
		FOR each_bid_row in c1(each_c_row.qid)
		LOOP
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_bid_row.advertiserId;

			if adv_balance >= each_bid_row.bid then 
				INSERT INTO temp_bid VALUES (each_bid_row.bid);
			end if;
		END LOOP;

		FOR each_c1_row in c1(each_c_row.qid)
		LOOP	
			SELECT balance INTO adv_balance
			FROM adv_finance
			WHERE adv_id = each_c1_row.advertiserId;

			if adv_balance >= each_c1_row.bid then
		
				SELECT COUNT(*) INTO temp_count
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
		
				if temp_count = 0 then
					INSERT INTO ads_hits VALUES (each_c1_row.advertiserId, 1);
				else
					UPDATE ads_hits
					SET hits = hits+1
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT hits INTO ad_hits
				FROM ads_hits
				WHERE adv_id = each_c1_row.advertiserId;
	
				SELECT ctc INTO adv_ctc
				FROM advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				adv_ctc := adv_ctc * 100;

				if ad_hits = 100 then
					UPDATE ads_hits
					SET hits = 0
					WHERE adv_id = each_c1_row.advertiserId;
				end if;

				SELECT count(*) INTO count_bidders
				FROM temp_output
				WHERE bid < each_c1_row.bid
				AND qid = each_c_row.qid;

				if count_bidders = 0 then
					second_bid := each_c1_row.bid;
				else
					SELECT MAX(bid) INTO second_bid
					FROM temp_bid
					WHERE bid < each_c1_row.bid;
				end if;
		
				if ad_hits > 0 AND ad_hits <= adv_ctc then
					adv_balance := adv_balance - second_bid;

					UPDATE adv_finance
					SET balance = adv_balance
					WHERE adv_id = each_c1_row.advertiserId;

					update_g_balance_algo_rank(each_c1_row.advertiserId, adv_balance);
				end if;

				SELECT budget INTO adv_budget
				FROM Advertisers
				WHERE advertiserId = each_c1_row.advertiserId;

				INSERT INTO task6_output VALUES (each_c_row.qid, rank, each_c1_row.advertiserId, adv_balance, adv_budget);
				rank := rank + 1;

				count_adv := count_adv + 1;
			end if;
			EXIT WHEN count_adv >= returned_rows;
		END LOOP;
		DELETE FROM temp_bid;
	END LOOP;
	clear_tables();
END second_price_auction_task6;
/
CREATE OR REPLACE PROCEDURE calculate_rank (query_id NUMBER)
AS
	temp_count NUMBER;
	product FLOAT;
	sq1 FLOAT;
	sq2 FLOAT;
	sq_root FLOAT;
	similarity FLOAT;
	quality_score FLOAT;
	ad_rank FLOAT;
	advertiser_ctc FLOAT;
	advertiser_budget FLOAT;
	cursor c is
		SELECT adv_id, bid
		FROM temp_adv;

	cursor c1(adv_id NUMBER) is
		SELECT keyword
		FROM Keywords
		WHERE advertiserId = adv_id;

	cursor c2 is
		SELECT query_count, advertiser_count
		FROM temp_table;
BEGIN
	FOR each_c_row IN c
	LOOP
		SELECT ctc, budget INTO advertiser_ctc, advertiser_budget 
		FROM advertisers
		WHERE advertiserId = each_c_row.adv_id;

		if (advertiser_budget >= each_c_row.bid) then
			FOR each_c1_row IN c1(each_c_row.adv_id)
			LOOP
				SELECT COUNT(*) INTO temp_count 
				FROM temp_table
				WHERE keyword = each_c1_row.keyword;

				if temp_count = 0 then
					INSERT INTO temp_table VALUES (each_c1_row.keyword, 0, 1);
				else
					UPDATE temp_table SET advertiser_count = advertiser_count+1
					WHERE keyword = each_c1_row.keyword;
				end if;	
			END LOOP;
				
			product := 0;
			FOR each_c2_row IN c2
			LOOP
				product := product + (each_c2_row.query_count * each_c2_row.advertiser_count);
			END LOOP;
	
			SELECT POWER(SUM(POWER(query_count, 2)), 0.5) INTO sq1
			FROM temp_table;

			SELECT POWER(SUM(POWER(advertiser_count, 2)), 0.5) INTO sq2 
			FROM temp_table;

			sq_root := sq1*sq2;
			similarity := product/sq_root;

			quality_score := similarity * advertiser_ctc;
			ad_rank := quality_score * each_c_row.bid;

			INSERT INTO temp_output VALUES (query_id, 
							each_c_row.adv_id, 
							ad_rank,
							0,
							0,
							each_c_row.bid, 
							quality_score); 

			UPDATE temp_table
			SET advertiser_count=0;
			COMMIT;
			
		END IF;	
	END LOOP;
	DELETE FROM temp_adv;
	COMMIT;
END calculate_rank;
/
CREATE OR REPLACE PROCEDURE calculate_rank_task3
AS
	ad_rank FLOAT;
	adv_balance FLOAT;
	cursor c is
		SELECT qid, advertiserId, quality_score
		FROM temp_output;
BEGIN
	FOR each_row in c
	LOOP
		SELECT balance INTO adv_balance
		FROM adv_finance
		WHERE adv_id = each_row.advertiserId;

		ad_rank := adv_balance * each_row.quality_score;

		UPDATE temp_output
		SET rank2 = ad_rank
		WHERE qid = each_row.qid AND
		advertiserId = each_row.advertiserId;
		
	END LOOP;
END calculate_rank_task3;
/
CREATE OR REPLACE PROCEDURE calculate_rank_task5
AS
	ad_rank FLOAT;
	phi FLOAT;
	budget_fraction FLOAT;
	adv_budget FLOAT;
	adv_balance FLOAT;
	e FLOAT;

	cursor c is
		SELECT *
		FROM temp_output;
BEGIN
	e := 2.71828;
	FOR each_row in c
	LOOP
		SELECT budget INTO adv_budget
		FROM Advertisers
		WHERE advertiserId = each_row.advertiserId;

		SELECT balance INTO adv_balance
		FROM adv_finance
		WHERE adv_id = each_row.advertiserId;

		budget_fraction := adv_balance/adv_budget;
		phi := each_row.bid * (1 - (POWER(e,-budget_fraction)));
		ad_rank := phi * each_row.quality_score;
		
		UPDATE temp_output
		SET rank3 = ad_rank
		WHERE qid = each_row.qid AND
		advertiserId = each_row.advertiserId;
		
	END LOOP;
END calculate_rank_task5;
/
CREATE OR REPLACE PROCEDURE tokenize (qid NUMBER, query VARCHAR2)
AS
	temp_count NUMBER;
	q_bid FLOAT;

        tokenized_query APEX_APPLICATION_GLOBAL.VC_ARR2;
	
	cursor b is
		SELECT keyword
		FROM temp_table;

	cursor c(key_word VARCHAR2) is
		SELECT advertiserId
		FROM Keywords
		WHERE keyword=key_word;
	
BEGIN
	tokenized_query := APEX_UTIL.STRING_TO_TABLE(query, ' ');
	
	FOR i IN 1..tokenized_query.count
	LOOP
		SELECT COUNT(*) INTO temp_count 
		FROM temp_table
		WHERE keyword = tokenized_query(i);

		if temp_count = 0 then
			INSERT INTO temp_table VALUES (tokenized_query(i), 1, 0);
		else
			UPDATE temp_table SET query_count = query_count+1
			WHERE keyword = tokenized_query(i);
		end if;	
	END LOOP;

	FOR each_row_in_b IN b
	LOOP
		FOR each_row_in_c in c(each_row_in_b.keyword)
		LOOP
			
			SELECT bid INTO q_bid
			FROM Keywords
			WHERE advertiserId = each_row_in_c.advertiserId AND
			keyword = each_row_in_b.keyword;

			SELECT COUNT(*) INTO temp_count
			FROM temp_adv
			WHERE adv_id=each_row_in_c.advertiserId;
			
			if temp_count = 0 then
				INSERT INTO temp_adv VALUES (each_row_in_c.advertiserId, q_bid);
			else
				UPDATE temp_adv
				SET bid = bid + q_bid
				WHERE adv_id = each_row_in_c.advertiserId;
			end if;
		END LOOP;
	END LOOP;
	calculate_rank(qid);
	DELETE FROM temp_table;
END tokenize;
/
CREATE OR REPLACE PROCEDURE start_process
AS
	cursor c is 
		SELECT qid, query 
		FROM queries; 
BEGIN 
	FOR each_query in c
	LOOP
		tokenize(each_query.qid, each_query.query);
	END LOOP;
	COMMIT;
END start_process;
/
exit();
