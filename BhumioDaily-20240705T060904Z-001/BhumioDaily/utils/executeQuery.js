const client = require('./dbutils');



// Define the executeQuery function to execute the SQL query
const statusNow = async () => {
  try {
    

    // Define the SQL query
    const queryText = `select l."mlsStatus" ,Count(distinct l."listingId")
    FROM listings as l
    LEFT JOIN listing_agents as la
    ON l.id=la."internalListingId"
    LEFT JOIN listing_agent_offices as lao
    ON la.id=lao."listingAgentId"
    LEFT JOIN offices as O
    ON lao."officeId"=O."officeId"
    LEFT JOIN office_addresses as OA
    ON O.id=OA."internalOfficeId"
    LEFT JOIN listing_prices as lp 
    ON l.id=lp."internalListingId"
    LEFT JOIN listing_dates ld 
    ON l.id=ld."internalListingId"
    LEFT JOIN listing_addresses as lad
    ON l.id= lad."internalListingId"
    LEFT join agents as a 
    on la."viewName" = a."name"
    LEFT join agent_addresses 
    ON agent_addresses."internalAgentId" = a.id and agent_addresses."type" = 'Office'
    
    where O."office" IS NOT NULL AND OA."street1" IS NOT NULL AND ld."onMarketDate">'2020-01-01' 

    and l."mlsStatus" in ('Active','Pending') 

    group by l."mlsStatus"

    union 
    select l."mlsStatus" ,Count(distinct l."listingId")
    from listings l join listing_dates ld ON l.id=ld."internalListingId"
    where l."mlsStatus" in ('Closed')   and ld."closeDate">CURRENT_DATE - interval'365 Days'
    group by l."mlsStatus" `;

    // Execute the query
    const { rows } = await client.query(queryText);

    // Cache the query result
   
    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};

const pendingToday = async () => {
  try {
    

    // Define the SQL query
    const queryText = `SELECT
    CURRENT_DATE - INTERVAL '1 Day' as "Today",
    count(case when "pendingDate"= CURRENT_DATE - INTERVAL '1 Day' then 1 end)as "Count"
 FROM
     listing_dates
 GROUP BY
     CURRENT_DATE - INTERVAL '1 Day';
  
    `;

    // Execute the query
    const { rows } = await client.query(queryText);

    
    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};
const closedToday = async () => {
  try {
   
    // Define the SQL query
    const queryText = `SELECT
    CURRENT_DATE - INTERVAL '1 Day' as "Today",
    count(case when "closeDate"= CURRENT_DATE - INTERVAL '1 Day' then 1 end)as "Count"
 FROM
     listing_dates
 
 GROUP BY
     CURRENT_DATE - INTERVAL '1 Day';
  
    `;

    // Execute the query
    const { rows } = await client.query(queryText);

   
    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};
const onMarketToday = async () => {
  try {
    // If query result is already cached, return it
  

    // Define the SQL query
    const queryText = `SELECT
    CURRENT_DATE - INTERVAL '1 Day' as "Today",
    count(case when "onMarketContractDate"= CURRENT_DATE - INTERVAL '1 Day' then 1 end)as "Count"
 FROM
     listing_dates
 
 GROUP BY
     CURRENT_DATE - INTERVAL '1 Day';
 
    `;

    // Execute the query
    const { rows } = await client.query(queryText);

  
    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};


const monthlyListing = async () => {
  try {
    // If query result is already cached, return it
  

    // Define the SQL query
    const queryText = `with cte as
    (SELECT 
    distinct l."listingId", TRIM(l."mlsStatus") AS "mlsStatus",
    la."viewName" AS "AgentName",la."type",la."subType",
    Case 
        when O."office" is NULL then 'No Brokerage'
        else O."office"
     END as "Brokerage",
    OA."street1" as "branchAddress",
    concat(OA."street1",',',OA."street2",' ',OA."city",',',OA."region",' ',LEFT(OA."postalCode",5)) as geospatial_branch_address,
    lp."currentPrice",ld."onMarketDate",
    
    (CASE WHEN trim(l."mlsStatus")='Closed' AND ld."closeDate" IS NULL then ld."statusChangeDate" else ld."closeDate" END) AS "final_date",
    
    trim(lad."shortAddress") as"shortAddress" 
    FROM listings as l
    LEFT JOIN listing_agents as la
    ON l.id=la."internalListingId"
    LEFT JOIN listing_agent_offices as lao
    ON la.id=lao."listingAgentId"
    LEFT JOIN offices as O
    ON lao."officeId"=O."officeId"
    LEFT JOIN office_addresses as OA
    ON O.id=OA."internalOfficeId"
    LEFT JOIN listing_prices as lp 
    ON l.id=lp."internalListingId"
    LEFT JOIN listing_dates ld 
    ON l.id=ld."internalListingId"
    LEFT JOIN listing_addresses as lad
    ON l.id= lad."internalListingId"
    LEFT join agents as a 
    on la."viewName" = a."name"
    LEFT join agent_addresses 
    ON agent_addresses."internalAgentId" = a.id and agent_addresses."type" = 'Office'
    
    where O."office" IS NOT NULL AND OA."street1" IS NOT NULL AND ld."onMarketDate">'2020-01-01' AND la."viewName" <> 'non member non member' AND la."viewName"<> 'non-member non member'
    )
    ,cte2 as (select *, dense_rank() over(partition by cte."listingId" order by cte."listingId") as rank1 from cte)
    ,cte3 as(select * ,
    dense_rank() over(partition by cte2."listingId",cte2."AgentName" order by cte2."branchAddress") as rank2
    from cte2 where   cte2.rank1=1)
    ,cte4 as( Select *,dense_rank() over(partition by cte3."shortAddress",cte3."AgentName",cte3."final_date" order by cte3."listingId") as rank3 from cte3 where cte3."rank2"=1)
    ,cteaddress as(select *,dense_rank() over(partition by cte4."listingId",cte4."AgentName" order by cte4."geospatial_branch_address" desc) as "Addressrank" from cte4)
    SELECT 
        TO_CHAR(cteaddress."onMarketDate", 'Mon') AS "Date",
        COUNT(DISTINCT CASE WHEN TO_CHAR(cteaddress."onMarketDate", 'YYYY') = '2022' THEN cteaddress."listingId" ELSE NULL END) AS "Twotwo",
        COUNT(DISTINCT CASE WHEN TO_CHAR(cteaddress."onMarketDate", 'YYYY') = '2023' THEN cteaddress."listingId" ELSE NULL END) AS "Twothree",
        COUNT(DISTINCT CASE WHEN TO_CHAR(cteaddress."onMarketDate", 'YYYY') = '2024' THEN cteaddress."listingId" ELSE NULL END) AS "Twofour"
    From cteaddress
    WHERE 
        cteaddress."onMarketDate" > CURRENT_DATE - INTERVAL '28 Months'
    GROUP BY 
        "Date"
    ORDER BY 
        CASE TO_CHAR(cteaddress."onMarketDate", 'Mon')
            WHEN 'Jan' THEN 1
            WHEN 'Feb' THEN 2
            WHEN 'Mar' THEN 3
            WHEN 'Apr' THEN 4
            WHEN 'May' THEN 5
            WHEN 'Jun' THEN 6
            WHEN 'Jul' THEN 7
            WHEN 'Aug' THEN 8
            WHEN 'Sep' THEN 9
            WHEN 'Oct' THEN 10
            WHEN 'Nov' THEN 11
            WHEN 'Dec' THEN 12
            ELSE 999
        END
    
`;

    // Execute the query
    const { rows } = await client.query(queryText);

  
    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};




const monthlyClosedListing = async () => {
  try {
    // If query result is already cached, return it
  

    // Define the SQL query
    const queryText = `with cte as
    (SELECT 
    distinct l."listingId", TRIM(l."mlsStatus") AS "mlsStatus",
    la."viewName" AS "AgentName",la."type",la."subType",
    Case 
        when O."office" is NULL then 'No Brokerage'
        else O."office"
     END as "Brokerage",
    OA."street1" as "branchAddress",
    concat(OA."street1",',',OA."street2",' ',OA."city",',',OA."region",' ',LEFT(OA."postalCode",5)) as geospatial_branch_address,
    lp."currentPrice",
    (CASE WHEN trim(l."mlsStatus")='Closed' AND ld."closeDate" IS NULL then ld."statusChangeDate" else ld."closeDate" END) AS "final_date",

    trim(lad."shortAddress") as"shortAddress" 
    FROM listings as l
    LEFT JOIN listing_agents as la
    ON l.id=la."internalListingId"
    LEFT JOIN listing_agent_offices as lao
    ON la.id=lao."listingAgentId"
    LEFT JOIN offices as O
    ON lao."officeId"=O."officeId"
    LEFT JOIN office_addresses as OA
    ON O.id=OA."internalOfficeId"
    LEFT JOIN listing_prices as lp 
    ON l.id=lp."internalListingId"
    LEFT JOIN listing_dates ld 
    ON l.id=ld."internalListingId"
    LEFT JOIN listing_addresses as lad
    ON l.id= lad."internalListingId"
    LEFT join agents as a 
    on la."viewName" = a."name"
    LEFT join agent_addresses 
    ON agent_addresses."internalAgentId" = a.id and agent_addresses."type" = 'Office'
    
    where O."office" IS NOT NULL AND OA."street1" IS NOT NULL AND ld."onMarketDate">'2020-01-01' AND la."viewName" <> 'non member non member' AND la."viewName"<> 'non-member non member'
    )
    ,cte2 as (select *, dense_rank() over(partition by cte."listingId" order by cte."listingId") as rank1 from cte)
    ,cte3 as(select * ,
    dense_rank() over(partition by cte2."listingId",cte2."AgentName" order by cte2."branchAddress") as rank2
    from cte2 where   cte2.rank1=1)
    ,cte4 as( Select *,dense_rank() over(partition by cte3."shortAddress",cte3."AgentName",cte3."final_date" order by cte3."listingId") as rank3 from cte3 where cte3."rank2"=1)
    ,cteaddress as(select *,dense_rank() over(partition by cte4."listingId",cte4."AgentName" order by cte4."geospatial_branch_address" desc) as "Addressrank" from cte4)
    SELECT 
        TO_CHAR(cteaddress."final_date", 'Mon') AS "Date",
        COUNT(DISTINCT CASE WHEN TO_CHAR(cteaddress."final_date", 'YYYY') = '2022' THEN cteaddress."listingId" ELSE NULL END) AS "Twotwo",
        COUNT(DISTINCT CASE WHEN TO_CHAR(cteaddress."final_date", 'YYYY') = '2023' THEN cteaddress."listingId" ELSE NULL END) AS "Twothree",
        COUNT(DISTINCT CASE WHEN TO_CHAR(cteaddress."final_date", 'YYYY') = '2024' THEN cteaddress."listingId" ELSE NULL END) AS "Twofour"
    From cteaddress
    WHERE 
        cteaddress."final_date" > CURRENT_DATE - INTERVAL '28 Months' AND cteaddress."mlsStatus" = 'Closed'
    GROUP BY 
        "Date"
    ORDER BY 
        CASE TO_CHAR(cteaddress."final_date", 'Mon')
            WHEN 'Jan' THEN 1
            WHEN 'Feb' THEN 2
            WHEN 'Mar' THEN 3
            WHEN 'Apr' THEN 4
            WHEN 'May' THEN 5
            WHEN 'Jun' THEN 6
            WHEN 'Jul' THEN 7
            WHEN 'Aug' THEN 8
            WHEN 'Sep' THEN 9
            WHEN 'Oct' THEN 10
            WHEN 'Nov' THEN 11
            WHEN 'Dec' THEN 12
            ELSE 999
        END
    
`;

    // Execute the query
    const { rows } = await client.query(queryText);

  
    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};




const topBroker = async () => {
  try {
   
    // Define the SQL query
    const queryText = `with cte as
    (SELECT 
    distinct l."listingId", TRIM(l."mlsStatus") AS "mlsStatus",
    la."viewName" AS "AgentName",la."type",la."subType",
    Case 
        when O."office" is NULL then 'No Brokerage'
        else O."office"
        
     END as "Brokerage",
    OA."street1" as "branchAddress",
    concat(OA."street1",',',OA."street2",' ',OA."city",',',OA."region",' ',LEFT(OA."postalCode",5)) as geospatial_branch_address,
    lp."currentPrice",
    (CASE WHEN trim(l."mlsStatus")='Closed' AND ld."closeDate" IS NULL then ld."statusChangeDate" else ld."closeDate" END) AS "final_date",
    (CASE WHEN la."subType"='Agent' then lp."currentPrice" else 0  END) AS "Volume",
    trim(lad."shortAddress") as"shortAddress" 
    FROM listings as l
    LEFT JOIN listing_agents as la
    ON l.id=la."internalListingId"
    LEFT JOIN listing_agent_offices as lao
    ON la.id=lao."listingAgentId"
    LEFT JOIN offices as O
    ON lao."officeId"=O."officeId"
    LEFT JOIN office_addresses as OA
    ON O.id=OA."internalOfficeId"
    LEFT JOIN listing_prices as lp 
    ON l.id=lp."internalListingId"
    LEFT JOIN listing_dates ld 
    ON l.id=ld."internalListingId"
    LEFT JOIN listing_addresses as lad
    ON l.id= lad."internalListingId"
    LEFT join agents as a 
    on la."viewName" = a."name"
    LEFT join agent_addresses 
    ON agent_addresses."internalAgentId" = a.id and agent_addresses."type" = 'Office'
    
    where O."office" IS NOT NULL AND OA."street1" IS NOT NULL AND ld."onMarketDate">'2020-01-01' AND la."viewName" <> 'non member non member' AND la."viewName"<> 'non-member non member'
    )
    ,cte2 as (select *, dense_rank() over(partition by cte."listingId" order by cte."listingId") as rank1 from cte)
    ,cte3 as(select * ,
    dense_rank() over(partition by cte2."listingId",cte2."AgentName" order by cte2."branchAddress") as rank2
    from cte2 where   cte2.rank1=1)
    ,cte4 as( Select *,dense_rank() over(partition by cte3."shortAddress",cte3."AgentName",cte3."final_date" order by cte3."listingId") as rank3 from cte3 where cte3."rank2"=1)
    ,cteaddress as(select *,dense_rank() over(partition by cte4."listingId",cte4."AgentName" order by cte4."geospatial_branch_address" desc) as "Addressrank" from cte4)
    Select distinct cteaddress."Brokerage",sum("Volume") as "Volume_Final",count(distinct cteaddress."listingId") from cteaddress where cteaddress."Addressrank"=1 AND cteaddress."final_date"> CURRENT_DATE - INTERVAL '365 days'
    group by cteaddress."Brokerage"
        order by "Volume_Final" DESC
      limit 10`;

    // Execute the query
    const { rows } = await client.query(queryText);

    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};



const filterStats = async () => {
  try {
   
    // Define the SQL query
    const queryText = `select lad."countyOrParish",l."mlsStatus" ,Count(distinct l."listingId")
    FROM listings as l
    LEFT JOIN listing_agents as la
    ON l.id=la."internalListingId"
    LEFT JOIN listing_agent_offices as lao
    ON la.id=lao."listingAgentId"
    LEFT JOIN offices as O
    ON lao."officeId"=O."officeId"
    LEFT JOIN office_addresses as OA
    ON O.id=OA."internalOfficeId"
    LEFT JOIN listing_prices as lp 
    ON l.id=lp."internalListingId"
    LEFT JOIN listing_dates ld 
    ON l.id=ld."internalListingId"
    LEFT JOIN listing_addresses as lad
    ON l.id= lad."internalListingId"
    LEFT join agents as a 
    on la."viewName" = a."name"
    LEFT join agent_addresses 
    ON agent_addresses."internalAgentId" = a.id and agent_addresses."type" = 'Office'
    
    where O."office" IS NOT NULL AND OA."street1" IS NOT NULL AND ld."onMarketDate">'2020-01-01' 

    and l."mlsStatus" in ('Active','Pending') 

    group by lad."countyOrParish",l."mlsStatus"

    union 
    select lad."countyOrParish", l."mlsStatus" ,Count(distinct l."listingId")
     FROM listings as l
    LEFT JOIN listing_agents as la
    ON l.id=la."internalListingId"
    LEFT JOIN listing_agent_offices as lao
    ON la.id=lao."listingAgentId"
    LEFT JOIN offices as O
    ON lao."officeId"=O."officeId"
    LEFT JOIN office_addresses as OA
    ON O.id=OA."internalOfficeId"
    LEFT JOIN listing_prices as lp 
    ON l.id=lp."internalListingId"
    LEFT JOIN listing_dates ld 
    ON l.id=ld."internalListingId"
    LEFT JOIN listing_addresses as lad
    ON l.id= lad."internalListingId"
    LEFT join agents as a 
    on la."viewName" = a."name"
    LEFT join agent_addresses 
    ON agent_addresses."internalAgentId" = a.id and agent_addresses."type" = 'Office'
    
    where O."office" IS NOT NULL AND OA."street1" IS NOT NULL and l."mlsStatus" in ('Closed')   and ld."closeDate">CURRENT_DATE - interval'365 Days'
    
    group by lad."countyOrParish",l."mlsStatus"`;

    // Execute the query
    const { rows } = await client.query(queryText);

    return rows; // Return the query result
  } catch (error) {
    console.error('Error executing query:', error);
    throw error; // Rethrow the error to be caught by the caller
  }
};



module.exports = {filterStats,monthlyClosedListing,topBroker,pendingToday,closedToday,onMarketToday,statusNow,monthlyListing};
