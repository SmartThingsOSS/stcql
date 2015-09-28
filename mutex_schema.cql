CREATE TABLE locks (
  lock_name varchar PRIMARY KEY,
  acquired_time timestamp,
  expiration_time timestamp,
  lock_holder varchar
);

