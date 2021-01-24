const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const playerDataAPI = require('./api/pffAPI');
const app = express();

app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use('/api/v1', playerDataAPI);

app.listen(3000);

//eslint-disable-next-line
console.log('express api started on port: 3000...');