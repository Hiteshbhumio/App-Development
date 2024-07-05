const express = require("express");
const router = express.Router();

const query = require("../controller/controller");
router.route("/data").get(async(req,res) => {
    try {
        const statusData = await query.status();
        const closedData = await query.closed();
        const pendingData = await query.pending();
        const marketData = await query.market();
        const monthly =await query.monthly();
        const monthlyClosedListing =await query.monthlyClosedListing();
        const topBroker =await query.topBroker();

        res.json({topBroker, statusData, closedData, pendingData, marketData,monthly,monthlyClosedListing });
    } catch (error) {
        console.error('Error fetching data:', error);
        res.status(500).json({ error: 'Error fetching data' });
    }
});

router.route("/office").get(async(req,res)=>
{
    try {
        const topBroker =await query.topBroker();
        res.json({topBroker});
    }
catch (error) {
    console.error('Error fetching data:', error);
    res.status(500).json({ error: 'Error fetching data' });

}}
);

router.route("/buyer").get(async(req,res)=>
{
    try {
        const Buyer =await query.Buyer();
        res.json({Buyer});
    }
catch (error) {
    console.error('Error fetching data:', error);
    res.status(500).json({ error: 'Error fetching data' });

}}
);

router.route("/Market").get(async(req,res)=>
    {
        try {
            const filterStats =await query.filterStats();
          

           
            res.json({filterStats});



        }
    catch (error) {
        console.error('Error fetching data:', error);
        res.status(500).json({ error: 'Error fetching data' });
    
    }}
    );

module.exports=router;