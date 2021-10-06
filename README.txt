#Script to compute impacted customers from Flooding situtaion in Thailand 2021-10

Update:
20211006 : Created by Tawan, T.

Why:
1.During 2021-10, there was heavy rains and storms coming to Thailand and there were flooded areas in many locations in northern, nort-eatern and central Thailand.
2.Information of the impacted customer in the flooded areas is beneficial to the operations for each product group management to manage and contain the damages to the customers and to arrange helps.

Input:
1.Flooded Areas - Map from GISTDA in SHP file - Daily (https://flood.gistda.or.th/)
2.Location of factories
3.Location of warehouses and transportation centers
4.Location of SSC customers
5.Location of TT customers
6.Location of CVM customers
7.Location of employees (from most frequent check-ins locations)

How:
1.Use Shape file (Polygon of the flooded areas) and locations as Lat Lng points
2.USe spiral join of geopandas to find the customer locations in the impacted area
