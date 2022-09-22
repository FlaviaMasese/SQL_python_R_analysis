/*SELECT state, lga, sector
FROM sect3_plantingw3
WHERE sector=1
ORDER BY state;
*/


/*
1. Analyze and visualize average expenditure by States, Local Government Area (LGA) and Sectors on the following ;
Kerosene, Palm kernel oil
Other liquid
Cooking fuel
Electricity
Candles
Firewood
Charcoal
Petrol
Diesel


s11bq4   HOW MUCH DID THE HH PURCHASE IN TOTAL?
"""
#%% import data

sect11b_harvestw3 = pd.read_csv("sect11b_harvestw3.csv")

sect11b_harvestw3.rename(columns={"s11bq4": "expend"}, inplace=True)  # .columns

*/

-- AVG expenditure of items by state
/*SELECT state, item_desc, AVG(s11bq4) AS avg_expenditure
FROM sect11b_harvestw3
WHERE item_desc in ('KEROSENE', 'PALM KERNEL OIL', 
                    'GAS', 'ELECTRICITY', 'CANDLES',
                    'FIREWOOD', 'CHARCOAL',
                    'PETROL', 'DIESEL'
                    )
GROUP BY state, item_desc
ORDER BY state, item_desc ASC;
*/

-- avg expenditure by lga
/*SELECT lga, item_desc, AVG(s11bq4) AS avg_expenditure
FROM sect11b_harvestw3
WHERE item_desc IN ('KEROSENE', 'PALM KERNEL OIL', 
                    'GAS', 'ELECTRICITY', 'CANDLES',
                    'FIREWOOD', 'CHARCOAL',
                    'PETROL', 'DIESEL'
                    )
GROUP BY lga, item_desc
ORDER BY lga, item_desc;
*/

-- avg expenditure by sector
/*SELECT sector, item_desc, AVG(s11bq4) AS avg_expenditure
FROM sect11b_harvestw3
WHERE item_desc IN ('KEROSENE', 'PALM KERNEL OIL', 
                    'GAS', 'ELECTRICITY', 'CANDLES',
                    'FIREWOOD', 'CHARCOAL',
                    'PETROL', 'DIESEL'
                    )
GROUP BY sector, item_desc
ORDER BY sector, item_desc;
*/



/*
"""
2. For each of the items above, identify the State, LGA and Sector with
the minimum and maximum expenditure and estimate how much it was.

"""
*/


/*
group by state and item
find the min EXPENDITURE
SELECT state
*/

-- minimum expenditure for each item in each state
/*SELECT state, item_desc, MIN(s11bq4) AS min_expenditure
FROM sect11b_harvestw3
WHERE item_desc IN ('KEROSENE', 'PALM KERNEL OIL', 
                    'GAS', 'ELECTRICITY', 'CANDLES',
                    'FIREWOOD', 'CHARCOAL',
                    'PETROL', 'DIESEL'
                    )
-- HAVING MIN(s11bq4)
GROUP BY state, item_desc
ORDER BY state, item_desc
*/

SELECT sector, item_desc, MAX(s11bq4) AS max_expenditure
FROM sect11b_harvestw3
GROUP BY item_desc, sector, s11bq4;



/*
3. Compute the number of households in each states and LGA that reported on
electricity expenditure and visualize the results
*/
SELECT state, COUNT(hhid) AS total_household
FROM sect11b_harvestw3
GROUP BY state;

SELECT lga, COUNT(hhid) AS total_household
FROM sect11b_harvestw3
GROUP BY lga;




