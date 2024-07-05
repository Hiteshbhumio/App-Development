const executeQuery = require("../utils/executeQuery");
const buyer = require("../utils/Buyer")

const status = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.statusNow();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};

const closed = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.closedToday();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};
const pending = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.pendingToday();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};
const market = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.onMarketToday();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};

const monthly = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.monthlyListing();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};

const monthlyClosedListing = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.monthlyClosedListing();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};



const topBroker = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.topBroker();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};
const  Buyer = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await buyer.Buyer();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};

const  filterStats = async () => {
    try {
        // Call the executeQuery function to get the rows
        const rows = await executeQuery.filterStats();

        // Return the fetched rows
        return rows;
    } catch (error) {
        // Handle errors
        console.error('Error fetching data:', error);
        throw new Error('Error fetching data');
    }
};


module.exports = {filterStats,monthlyClosedListing,Buyer,topBroker, status,closed,pending,market,monthly };
