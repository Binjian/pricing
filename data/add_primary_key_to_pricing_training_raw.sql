PRAGMA foreign_keys=off;
BEGIN TRANSACTION;
ALTER TABLE price_training_raw_2024_usd RENAME TO old_table;
CREATE TABLE price_training_raw_2024_usd (
    dispatch_id INTEGER NOT NULL PRIMARY KEY
    );
INSERT INTO price_training_raw_2024_usd SELECT * FROM old_table;
COMMIT;
PRAGMA foreign_keys=on;