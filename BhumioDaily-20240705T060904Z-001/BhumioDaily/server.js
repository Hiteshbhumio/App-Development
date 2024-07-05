const express =require("express");
const cors = require('cors');
const app =express();
port=3000;

const router =require("./router/router");


app.use(express.json());
app.use(cors()); 
app.use("/api/auth",router);



app.listen(port,()=>{
    console.log("server is running");
});