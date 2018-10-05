CREATE KEYSPACE cc WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use cc;

CREATE TYPE Address (
	id text, 
	street text, 
	city text, 
	postal_code text, 
	county text, 
	country text, 
	state text
);


DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
	id text primary key,
	first_name text,
	last_name text,
	email text,
	gender text,
	age int,
	dob date,
	address Address,
	--- Derived columns
	amount_upper_threshold double,
	amount_lower_threshold double
);



DROP TABLE IF EXISTS merchant;
CREATE TABLE merchant (
	id text primary key,
	name text,
	ticker text,
	address Address
);

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
	id text,
	merchant_id text, 
	customer_id text, 
	location Address,
	location_id text,
	amount double, 
	category text, 
	timestamp timestamp,
	-- Derived fields
	flag_threshold double, 
	flag_ml double,
	score double,
	overruled double,
	overruled_comment text,
	overruled_date timestamp,
	primary key ((customer_id), timestamp, id)
)  WITH CLUSTERING ORDER BY(timestamp DESC);


