const sqlite3 = require('sqlite3');
const {Client} = require('pg');
require('dotenv').config();


const sqliteConn = new sqlite3.Database('src/BusAccesosDB.db')


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

sqliteConn.all(`SELECT access_date,balance_new, balance_old, qr_creation_date as creation_date, device_id, discount_amount, discount_concept, latitude, longitude, passengers
,payment_company, payment_company_id, payment_method, payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title, no_bank, bank, no_bank_flag
FROM BusAccessTable`, (err, rows) => {
  if (err) {
    console.error(err);
    return;
  }

  // Utiliza una consulta INSERT en la base de datos PostgreSQL para insertar los datos
  rows.forEach((row) => {
    if(row.device_id == 'DDnrdK4iyl4D4fPfsl') {
        row.device_id = 241
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

    if(row.device_id == 'DDnrdK4iyl4D4fPfsl') {
      row.device_id = 241
    }


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

  });

  console.log(values);

  new Promise(function (resolve, reject) {
    const insertArray = pgConn.query(
        `INSERT INTO tra_bus_access (access_date, balance_new, balance_old, creation_date, device_id, discount_amount, discount_concept,
          latitude, longitude, passengers, payment_company, payment_company_id, payment_method, 
          payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title, no_bank, bank, no_bank_flag)
         SELECT access_date, balance_new, balance_old, creation_date, device_id, discount_amount, discount_concept,
         latitude, longitude, passengers, payment_company, payment_company_id, payment_method,
         payment_method_id, qr_version, subtotal, ticket_id, user_category, total, user_id, user_profile, user_title, no_bank, bank, no_bank_flag
         FROM jsonb_to_recordset($1::jsonb) AS t (access_date timestamp, balance_new int, balance_old int, creation_date bigint, device_id int,
            discount_amount int, discount_concept text, latitude float, longitude float,
            passengers int, payment_company int, payment_company_id int, payment_method int,
            payment_method_id text, qr_version int, subtotal float, ticket_id int,
            user_category text, total float, user_id text, user_profile text, user_title text, no_bank int, bank int, no_bank_flag bool)`,
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

// node src/index.js
