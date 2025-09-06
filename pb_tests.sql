\include_relative pb_parser.sql

create or replace function pg_temp.assert_equals(testcase text, expected anyelement, actual anyelement, failures text[]) returns text[] language plpgsql as $$
    begin
        if expected is null and actual is null or expected = actual then --  AND pg_typeof(expected) = pg_typeof(actual) then
            return failures;
        else
            return array_append(failures, 'Test `' || testcase || '` failed. Expected ' || coalesce('`' || expected::text || '`', '¤') || ' but got ' || coalesce('`' || actual::text || '`', '¤'));
        end if;
    end;
$$;


create or replace function pg_temp.test() returns table (failure text) language plpgsql as $$
    declare
        payload bytea = '\x080112056f6c6f6c6f1d560e49402144174154fb2109402a40082a10808080c09af9eeab3a182b20808080c09af9eeab3a28583080808080b5f2ddd7743d2d00000041000000a8c9bb573a4d2e00000051000000a8c9bb573a323008d6ffffffffffffffff0110808080c0e58691d4c501285730ffffffffb4f2ddd7744dd2ffffff51000000583644a8c5';
        failures text[] = '{}';
    begin
        failures = pg_temp.assert_equals('pb_bool_strict()', true, pg_temp.pb_bool_strict(payload, 1), failures);
        failures = pg_temp.assert_equals('pb_bool() wrong type', null, pg_temp.pb_bool(payload, 2), failures);
        failures = pg_temp.assert_equals('pb_bool() missing', null, pg_temp.pb_bool(payload, 10), failures);
        failures = pg_temp.assert_equals('pb_text_strict()', 'ololo', pg_temp.pb_text_strict(payload, 2), failures);
        failures = pg_temp.assert_equals('pb_text() wrong type', null, pg_temp.pb_text(payload, 3), failures);
        failures = pg_temp.assert_equals('pb_text() missing', null, pg_temp.pb_text(payload, 10), failures);
        failures = pg_temp.assert_equals('pb_float_strict()', 3.1415::real, pg_temp.pb_float_strict(payload, 3), failures);
        failures = pg_temp.assert_equals('pb_float() wrong type', null, pg_temp.pb_float(payload, 4), failures);
        failures = pg_temp.assert_equals('pb_float() missing', null, pg_temp.pb_float(payload, 10), failures);
        failures = pg_temp.assert_equals('pb_double_strict()', 3.1415926535::double precision, pg_temp.pb_double_strict(payload, 4), failures);
        failures = pg_temp.assert_equals('pb_double() wrong type', null, pg_temp.pb_double(payload, 5), failures);
        failures = pg_temp.assert_equals('pb_double() missing', null, pg_temp.pb_double(payload, 10), failures);
        failures = pg_temp.assert_equals('pb_int_strict() 32 bit positive', 42::bigint, pg_temp.pb_int_strict(payload, 5, 1), failures);
        failures = pg_temp.assert_equals('pb_int_strict() 32 bit negative', -42::bigint, pg_temp.pb_int_strict(payload, 6, 1), failures);
        failures = pg_temp.assert_equals('pb_int_strict() 64 bit positive', 424242424242424242424242, pg_temp.pb_int_strict(payload, 5, 2), failures);
        failures = pg_temp.assert_equals('pb_int_strict() 64 bit negative', -424242424242424242424242, pg_temp.pb_int_strict(payload, 6, 2), failures);
        failures = pg_temp.assert_equals('pb_int() wrong type', null, pg_temp.pb_int(payload, 5, 7), failures);
        failures = pg_temp.assert_equals('pb_int() missing', null, pg_temp.pb_int(payload, 5, 10), failures);

        for failure in select failures loop
            return next;
        end loop;
    end;
$$;

select * from pg_temp.test();