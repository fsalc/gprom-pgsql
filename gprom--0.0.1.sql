CREATE FUNCTION snapshot(integer)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;