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
