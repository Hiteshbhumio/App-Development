const { Client } = require('pg');

// Create a new client instance
const client = new Client({
    host:'mls-sync-db.c9buzukk3esw.us-east-1.rds.amazonaws.com',
    user:'analytics',
    password:'d7424aeb9b888be2',
    database:'mls_data',
    port: 5432,
});

// Connect to the PostgreSQL database
client.connect()
  .then(() => console.log('Connected to database'))
  .catch(error => console.error('Error connecting to database:', error));

module.exports = client;
