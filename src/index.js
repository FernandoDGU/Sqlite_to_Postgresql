const sqlite3 = require('sqlite3');
const {Client} = require('pg');
require('dotenv').config();


const sqliteConn = new sqlite3.Database('src/BusAccesosDB.db')
// dev
// const pgConn = new Client({
//   user: 'ixoqltkgxmsohk',
//   host: 'ec2-3-230-122-20.compute-1.amazonaws.com',
//   database: 'd99cim5pbrplh8',
//   password: '76e6024732f283c7e13390b5ffcf4e5b297910b14256d974fc6d7975be8ece89',
//   port: 5432,
//   ssl: { rejectUnauthorized: false }
// });

// prod
const pgConn = new Client({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: 5432,
  ssl: { rejectUnauthorized: false }
});

pgConn.connect();

let values = [];

// sqliteConn.serialize(function () {
//   sqliteConn.all("select name from sqlite_master where type='table'", function (err, tables) {
//       console.log(tables);
//   });
// });
sqliteConn.all(`SELECT access_date,balance_new, balance_old, qr_creation_date as creation_date, device_id, discount_amount, discount_concept, latitude, longitude, passengers
,payment_company, payment_company_id, payment_method, payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title, no_bank, bank, no_bank_flag
FROM BusAccessTable`, (err, rows) => {
  if (err) {
    console.error(err);
    return;
  }

  // Utiliza una consulta INSERT en la base de datos PostgreSQL para insertar los datos
  rows.forEach((row) => {
    if(row.device_id == 'T2fTLkXRhmUWvuNx3S') {
        row.device_id = 67
    }

    if(row.payment_company == 'MIA') {
      row.payment_company = 1
    }

    if(row.payment_company == 'URBANI') {
      row.payment_company = 2
    }

    if(row.payment_company_id == 'STC Metrorrey') {
      row.payment_company_id = 1
    }

    if(row.payment_company_id == 'Inatel') {
      row.payment_company_id = 2
    }

    if(row.payment_method == 'TARJETA') {
      row.payment_method = 1
    }else if (row.payment_method == 'QR') {
      row.payment_method = 2
    }

    if(row.device_id == 'T2fTLkXRhmUWvuNx3S') {
      row.device_id = 67
    }

    // values = [
    //   row.access_date, row.balance_new, row.balance_old, row.creation_date, row.device_id, row.discount_amount, row.discount_concept, row.latitude, row.longitude, row.passengers,
    //   row.payment_company, row.payment_company_id, row.payment_method, row.payment_method_id, row.qr_version, row.subtotal, row.ticket_id, row.user_category, row.total, row.user_id, row.user_profile, row.user_title,
    // ];

    values.push(({
      access_date: row.access_date, 
      balance_new: row.balance_new,
      balance_old: row.balance_old,
      creation_date: row.creation_date,
      device_id: row.device_id,
      discount_amount: row.discount_amount,
      discount_concept: row.discount_concept,
      latitude : row.latitude,
      longitude: row.longitude,
      passengers: row.passengers,
      payment_company: row.payment_company,
      payment_company_id: row.payment_company_id,
      payment_method: row.payment_method,
      payment_method_id: row.payment_method_id,
      qr_version: row.qr_version,
      subtotal: row.subtotal,
      ticket_id: row.ticket_id,
      user_category: row.user_category, 
      total: row.total, 
      user_id: row.user_id, 
      user_profile: row.user_profile, 
      user_title: row.user_title,
      no_bank: row.no_bank,
      bank: row.bank,
      no_bank_flag: row.no_bank_flag
    }))

    // console.log(values);
    // const query = `INSERT INTO tra_bus_access (access_date, balance_new, balance_old, creation_date, device_id, discount_amount, discount_concept,
    //     latitude, longitude, passengers, payment_company, payment_company_id, payment_method, 
    //     payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title) 
    //     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`;
    // pgConn.query(query, values, (err) => {
    //   if (err) {
    //     console.error(err);
    //     return;
    //   }
    //   console.log(`Inserted row with ID ${row.id}`);
    // });
  });

  console.log(values);

  new Promise(function (resolve, reject) {
    const insertArray = pgConn.query(
        `INSERT INTO tra_bus_access (access_date, balance_new, balance_old, creation_date, device_id, discount_amount, discount_concept,
          latitude, longitude, passengers, payment_company, payment_company_id, payment_method, 
          payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title)
         SELECT access_date, balance_new, balance_old, creation_date, device_id, discount_amount, discount_concept,
         latitude, longitude, passengers, payment_company, payment_company_id, payment_method,
         payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title
         FROM jsonb_to_recordset($1::jsonb) AS t (access_date timestamp, balance_new int, balance_old int, creation_date bigint, device_id int,
            discount_amount int, discount_concept text, latitude float, longitude float,
            passengers int, payment_company int, payment_company_id int, payment_method int,
            payment_method_id text, qr_version int, subtotal float, ticket_id int,
            user_category text, total float, user_id text, user_profile text, user_title text)`,
        [
            JSON.stringify(values),
        ], (err, res) => {
            console.log("err ========================>", err);
            console.log("res ========================>", res);
            if (res?.rowCount >= 1) {
                resolve({inserted: true});
            }
        }
    );
    return insertArray;
    }).then((result) => {
      console.log("insertArray response -------------->", result);
      if (result.inserted) {
          // Cierra las conexiones
          sqliteConn.close();
          pgConn.end();
      }
    });
  // Cierra las conexiones
         
});

// sqliteConn.close();
// pgConn.end();







// node src/index.js
