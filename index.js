const express = require("express");
const crypto = require("crypto");
const app = express();

// It's important to use this middleware otherwise server won't be able to parse JSON and it will throw
// TypeError: Cannot destructure property 'name' of 'req.body' as it is undefined.
app.use(express.json());

const mysql = require("mysql");
require("dotenv").config();

const shardOnePool = mysql.createPool({
  connectionLimit: 10,
  host: process.env.SHARD_1_DB_HOST,
  user: process.env.SHARD_1_DB_USER,
  password: process.env.SHARD_1_DB_PASSWORD,
  database: process.env.SHARD_1_DB_DATABASE,
  port: 3306,
  ssl: true,
});

const shardTwoPool = mysql.createPool({
  connectionLimit: 10,
  host: process.env.SHARD_2_DB_HOST,
  user: process.env.SHARD_1_DB_USER,
  password: process.env.SHARD_1_DB_PASSWORD,
  database: process.env.SHARD_1_DB_DATABASE,
  port: 3306,
  ssl: true,
});

const calculateShard = (name) => {
  return name.charAt(0).toLowerCase().charCodeAt(0) <= 109
    ? shardOnePool
    : shardTwoPool;

  // can also use this logic to determine which shard to insert into
  //   const hash = crypto
  //     .createHash("sha256")
  //     .update(name + email)
  //     .digest("hex");
  //   const mod2 = parseInt(hash, 16) % 2;
};

const insertUser = (shard, name, email, callback) => {
  const sql = "INSERT INTO users (name, email) VALUES (?, ?)";
  const values = [name, email];

  shard.getConnection((err, connection) => {
    if (err) return callback(err);

    console.log("Connected to MySQL database!");

    connection.query(sql, values, (error, results) => {
      if (error) return callback(error);

      console.log(`Inserted ${results.affectedRows} row(s)`);
      callback(null, results);
    });
  });
};

const GetUsers = (shard, callback) => {
  shard.getConnection((err, connection) => {
    if (err) throw err;
    console.log("Connected to Shard!");

    connection.query("SELECT * FROM users", (error, results) => {
      connection.release();
      if (error) throw error;
      console.log(results);
      callback(null, results);
    });
  });
};

app.get("/", (req, res) => {
  let users = [];
  GetUsers(shardOnePool, (err, result) => {
    if (err) throw err;
    users.push(result);

    GetUsers(shardTwoPool, (err, result) => {
      if (err) throw err;
      users.push(result);
      res.send(users);
    });
  });
});

app.post("/users", (req, res) => {
  const { name, email } = req.body;
  const shard = calculateShard(name);

  insertUser(shard, name, email, (err, results) => {
    if (err) throw err;

    res.send("User created!");
  });
});

app.listen(5000, () => {
  console.log("Server is running on port 5000");
});
