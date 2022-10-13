-- Example DWH

DROP TABLE IF EXISTS dim_transaction_order;
CREATE TABLE dim_transaction_order (
	id_transaction INT NOT NULL,
	id_customer INT8 NOT NULL,
	name_customer VARCHAR(255),
	birthdate_customer VARCHAR(255),
	gender_customer VARCHAR(255),
	country_customer VARCHAR(255),
	date_transaction VARCHAR(255), 
	product_kategory VARCHAR(255),
	product_transaction VARCHAR(255),
	amount_transaction INT
	);
