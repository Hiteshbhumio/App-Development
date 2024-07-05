const client = require('./dbutils');

const Buyer = async () => {
    try {
     
      // Define the SQL query
      const queryText = `with cte as
      (SELECT 
      distinct l."listingId", TRIM(l."mlsStatus") AS "mlsStatus",l."propertyClass",
      la."viewName" AS "AgentName",lad."city",lad."countyOrParish",
      
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
      
      
      where O."office" IS NOT NULL AND OA."street1" IS NOT NULL AND la."viewName" <> 'non member non member' AND la."viewName"<> 'non-member non member'
      AND ld."statusChangeDate"> CURRENT_DATE -interval'365 Days' and l."mlsStatus"='Closed' and la."subType"='Agent' and la."type"='Buyer RE Agent'
        
      )
      ,cte2 as (select *, dense_rank() over(partition by cte."listingId" order by cte."listingId") as rank1 from cte)
      ,cte3 as(select * ,
      dense_rank() over(partition by cte2."listingId",cte2."AgentName" order by cte2."branchAddress") as rank2
      from cte2 where   cte2.rank1=1)
      ,cte4 as( Select *,dense_rank() over(partition by cte3."shortAddress",cte3."AgentName",cte3."final_date" order by cte3."listingId") as rank3 from cte3 where cte3."rank2"=1)
      ,cteaddress as(select *,dense_rank() over(partition by cte4."listingId",cte4."AgentName" order by cte4."geospatial_branch_address" desc) as "Addressrank" from cte4)
      Select distinct cteaddress."listingId",cteaddress."propertyClass",cteaddress."AgentName",cteaddress."city",cteaddress."countyOrParish",cteaddress."currentPrice", cteaddress."shortAddress" ,cteaddress."Brokerage",cteaddress."branchAddress"  from cteaddress where cteaddress."Addressrank"=1 
    `;
  
      // Execute the query
      const { rows } = await client.query(queryText);
  
     
      return rows; // Return the query result
    } catch (error) {
      console.error('Error executing query:', error);
      throw error; // Rethrow the error to be caught by the caller
    }
  };
  
  module.exports = {Buyer}