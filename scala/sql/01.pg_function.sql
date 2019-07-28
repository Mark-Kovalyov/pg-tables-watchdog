DROP TABLE IF EXISTS DIFF_TYPES CASCADE;

CREATE TABLE DIFF_TYPES(
    s smallint,
    i integer,
    se serial ,
    c char(1),
    v varchar(2),
    t text,
    r real,
    f float,
    n numeric(38,2),
    f_d date,
    f_t time,
    f_ts timestamp,
    f_tsz timestamptz,
    f_int interval
);


CREATE OR REPLACE FUNCTION DIFF_TYPES_FUNC() RETURNS TRIGGER AS $$
BEGIN
  RAISE NOTICE ':: Trigger on DIFF_TYPES called!';
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS DIFF_TYPES_TRIG ON DIFF_TYPES CASCADE;

CREATE TRIGGER DIFF_TYPES_TRIG AFTER INSERT OR UPDATE OR DELETE ON DIFF_TYPES FOR EACH ROW EXECUTE PROCEDURE DIFF_TYPES_FUNC();


CREATE OR REPLACE FUNCTION mtn_func_testtext() RETURNS TRIGGER AS
$$
BEGIN
    IF TG_OP = 'INSERT' THEN
       INSERT INTO mtn_func_testtext(mtn_ts, mtn_op, t) VALUES(CURRENT_TIMESTAMP, 'I', NEW.t);
    ELSIF TG_OP = 'UPDATE' THEN
       INSERT INTO mtn_func_testtext(mtn_ts, mtn_op, t) VALUES(CURRENT_TIMESTAMP, 'U', NEW.t);
    ELSIF TG_OP = 'DELETE' THEN
       INSERT INTO mtn_func_testtext(mtn_ts, mtn_op) VALUES(CURRENT_TIMESTAMP, 'D');
    END IF;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION dup(in int, out f1 int, out f2 text) AS $$
   SELECT $1, CAST($1 AS text) || ' is text' $$
   LANGUAGE SQL;

CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$
    BEGIN
      RETURN i + 1;
    END;
$$ LANGUAGE plpgsql;

