CREATE VOLATILE TABLE vt_Sales AS
(
SELECT a.*

FROM
	 (
	 SELECT
		 pop.Client_ID
		 , t01.Sales_ID
		 , 'Chequebook' AS DATASOURCE
		 , t01.Start_Date
		 , t02.Position_Held

	 FROM 		DB.population AS pop

	 LEFT JOIN 	DB.Client_Links AS t01
	 ON 		pop.Client_ID=t01.Client_ID

	 LEFT JOIN 	DB.LU_Client_Links AS t02
	 ON 		t01.Link_Type_Id=t02.Link_Type_Id

	 WHERE 		t01.Link_Type_Id IN(1,5,3)

	 UNION

	 SELECT
		 pop.Client_ID
		 , t01.Sales_ID
		 , 'Registerbook' AS DATASOURCE
		 , t01.Start_Date
		 , t02.Position_Held

	 FROM 		DB.population AS pop

	 INNER JOIN 	DB.Associates AS t01
	 ON 		pop.Client_ID=t01.Client_ID

	 LEFT JOIN 	DB.Party_Relationship AS t02
	 ON 		t02.Party_Id=t01.Party_Id
	 AND 		t02.Related_Prty_Id=t01.Related_Prty_Id

	 WHERE (t02.Party_Relationship_End_Date > Current_Date OR t02.Party_Relationship_End_Date IS NULL)
	 AND Position_Held LIKE '%Sales%'
	 
	 QUALIFY Row_Number() Over(PARTITION BY Client_ID, Sales_ID ORDER BY Party_Relationship_End_Date DESC)=1
	 ) a

QUALIFY Row_Number() Over(PARTITION BY Client_ID, Sales_ID ORDER BY Position_Held DESC)=1

) WITH DATA
PRIMARY INDEX(Client_ID)
ON COMMIT PRESERVE ROWS
;

--Grabbing first and last name details of Sales_person
CREATE VOLATILE TABLE vt_name AS
(
SELECT
	 pop.Client_ID
	 , pop.Sales_ID
	 , DATASOURCE
	 , Start_Date
	 , Position_Held
	 , t01.First_name
	 , t01.Last_name

FROM 		vt_Sales AS pop

LEFT JOIN 	DB.client_name AS t01
ON 		pop.Sales_ID=t01.Client_ID
AND 		t01.Client_name_status = 50
AND 		t01.Client_current_ind = 'Y'

INNER JOIN 	DB.name_type AS t02
ON 		t01.name_code=t02.name_code
AND 		t02.name_code IN (50,100)

WHERE 		(t01.First_name IS NOT NULL AND t01.First_name <>' ')

QUALIFY Row_Number() Over(PARTITION BY Client_ID, Sales_ID ORDER BY Client_name_created_date DESC)=1
)
WITH DATA
PRIMARY INDEX(Client_ID, Sales_ID)
ON COMMIT PRESERVE ROWS 
;


--Date of birth details
CREATE VOLATILE TABLE vt_dob AS
(
SELECT
	 pop.Client_ID
	 , pop.Sales_ID
	 , t01.birth_day
	 , t01.birth_month
	 , t01.birth_year
	, CASE
 			WHEN t01. birth_year BETWEEN 1 AND 9999
 			AND t01.birth_month BETWEEN 1 AND 12
 			AND t01.birth_day BETWEEN 1 AND CASE

			WHEN t01.birth_month IN (1,3,5,7,8,10,12) THEN 31

			WHEN t01.birth_month IN (4,6,9,11) THEN 30

			ELSE 28 + CASE

							WHEN (t01.birth_year MOD 4 = 0 AND t01.birth_year MOD 100 <> 0)

							OR t01.birth_year MOD 400 = 0

						THEN 1

					ELSE 0

					END

				END
 
			THEN ((t01.birth_year-1900)*10000 + (t01.birth_month * 100) + t01.birth_day (DATE) )
 			ELSE NULL
 			END AS actual_birth_date

FROM 		vt_name AS pop

INNER JOIN 	DB.Client AS t01
ON 		pop.Sales_ID = t01.Client_ID

QUALIFY Row_Number() Over(PARTITION BY Client_ID, Sales_ID ORDER BY actual_birth_date DESC)=1
) WITH DATA
PRIMARY INDEX(Client_ID)
ON COMMIT PRESERVE ROWS;



CREATE VOLATILE TABLE vt_balance AS
(
SELECT
	pop.Client_ID
	, pop.Sales_ID
	, Coalesce(t01.Current_Bal_Amt,0) AS EFT_balance
	, Coalesce(t02.Cash_balance,0) AS Cash_balance

FROM 	vt_name AS pop

LEFT JOIN
		 (SELECT
		 	d.Client_ID
			,Sum(d.Acnt_Bal_Amt) AS Current_Bal_Amt

		 FROM 	DB.Acnt_History d

		 WHERE Current_Date BETWEEN d. Acnt_Efctv_Strt_Dt AND
		d. Acnt_Efctv_End_Dt AND d.Act_Prdct_Cd = 'EFT'
		 AND Client_ID IN (SELECT Client_ID FROM vt_name)

		 GROUP BY 1
		 ) t01
ON 		t01.Client_ID=pop.Client_ID

LEFT JOIN
		 (SELECT
		 	fx.Client_ID
			,Sum(fx.Totl_Owing_Amt)-Sum(fx.Totl_Collection_Amt) AS Cash_balance

		 FROM 		Db.Fin_trn fx

		 INNER JOIN DB.Client_Account ac
		 ON 		ac.Acnt_Id = fx.Acnt_Id
		 AND 		ac.Client_account_code = 141

		 WHERE 		fx.Transaction_date <= Current_Date
		 AND 		fx.Acnt_ID IN (SELECT Client_ID FROM vt_name)
		 HAVING 	Cash_balance > 0.00

		 GROUP BY 1
		 ) t02
ON 		t02.Client_ID =pop.Client_ID
)
WITH DATA
PRIMARY INDEX(Client_ID)
ON COMMIT PRESERVE ROWS
;


CREATE VOLATILE TABLE vt_combined AS
(
SELECT
	 pop.Client_ID
	 , pop.Sales_ID
	 , pop.DATASOURCE
	 , pop.Start_Date
	 , pop.Position_Held
	 , t01.First_name
	 , t01.Last_name
	 , t02.actual_birth_date
	 , t03.EFT_balance
	 , t03.Cash_balance

FROM 		vt_sales AS pop

INNER JOIN 	vt_name AS t01
ON 		pop.Client_ID =t01.Client_ID
AND 		pop.Sales_ID=t01.Sales_ID

INNER JOIN 	vt_dob AS t02
ON 		pop.Client_ID =t02.Client_ID
AND 		pop.Sales_ID=t02.Sales_ID

LEFT JOIN 	vt_balance AS t03
ON 		pop.Client_ID =t03.Client_ID
AND 		pop.Sales_ID=t03.Sales_ID

) WITH DATA
PRIMARY INDEX (Client_ID,Sales_ID)
ON COMMIT PRESERVE ROWS
;

SELECT * FROM vt_combined
ORDER BY 1
;
