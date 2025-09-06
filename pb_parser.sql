-- Protobuf encoding documentation https://protobuf.dev/programming-guides/encoding/
-- Protobuf online message encoder/decoder https://protobufpal.com/ or https://yura415.github.io/js-protobuf-encode-decode/

--TODO
-- 1. support sint32 and sint64 wiz Zig-Zag encoding
-- 2. support repeated fields
-- 3. add pg_temp to search_path https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH


-- to see debug messages:
-- set client_min_messages = 'DEBUG1';

\set QUIET on

-- select pg_temp._pb_parse_varint('\x0a2462383861663337662d626530642d343037382d386237322d333037653834363736343939', 1);
-- from_index and next_index counts from 1 byte
create or replace function pg_temp._pb_parse_varint(pb bytea, from_index int) returns table (next_index int, result bit varying) language plpgsql as $$
    declare
        b int;
        len int = octet_length(pb);
    begin
        next_index = from_index;
        result = B'';
        loop
            b = get_byte(pb, next_index - 1);
            raise debug 'Byte %', b;
            result = b::bit(7) || result;
            raise debug 'Result %', result;
            next_index = next_index + 1;

            exit when b < 128;
            if next_index > len then
                raise exception 'Failed to parse varint: byte sequence didnt end with trailing byte with 0 in most significant bit.';
            end if;
            if next_index - from_index > 10 then -- invalid varint
                raise exception 'Failed to parse varint: sequence can`t be longer than 10 bytes.';
            end if;
        end loop;
        return next;
    end;
$$;

create or replace function pg_temp._pb_parse_32(payload bytea, from_index int) returns bit(32) language plpgsql as $$
    begin
        return get_byte(payload, from_index + 2)::bit(8)
            || get_byte(payload, from_index + 1)::bit(8)
            || get_byte(payload, from_index    )::bit(8)
            || get_byte(payload, from_index - 1)::bit(8);
    end;
$$;

create or replace function pg_temp._pb_parse_64(payload bytea, from_index int) returns bit(64) language plpgsql as $$
    begin
        return get_byte(payload, from_index + 6)::bit(8)
            || get_byte(payload, from_index + 5)::bit(8)
            || get_byte(payload, from_index + 4)::bit(8)
            || get_byte(payload, from_index + 3)::bit(8)
            || get_byte(payload, from_index + 2)::bit(8)
            || get_byte(payload, from_index + 1)::bit(8)
            || get_byte(payload, from_index    )::bit(8)
            || get_byte(payload, from_index - 1)::bit(8);
    end;
$$;

create or replace function pg_temp._pb_decode_uint32(bits bit(32)) returns decimal(10, 0) language plpgsql as $$
    begin
        return bits::bigint::decimal(10, 0);
    end;
$$;

create or replace function pg_temp._pb_decode_int64(bits bit(64)) returns bigint language plpgsql as $$
    begin
        return bits::bigint;
    end;
$$;

create or replace function pg_temp._pb_decode_uint64(bits bit(64)) returns decimal(20, 0) language plpgsql as $$
    begin
        return substring(bits from 1 for 63)::bigint::decimal(20, 0) * 2 + get_bit(bits, 63);
    end;
$$;

create or replace function pg_temp._pb_decode_float(bits bit(32)) returns real language plpgsql as $$
    declare
        sign int = get_bit(bits, 0);
        exponent int = substring(bits from 2 for 8)::int;
        mantissa int = substring(bits from 10 for 23)::int;
        result real;
    begin
        raise debug 'sign %, mantissa %, exponent %', sign, mantissa, exponent;

        if mantissa = 0 AND exponent = 0 AND sign = 0 then
            return 0.0;
        end if;
        if mantissa = 0 AND exponent = 0 AND sign = 1 then
            return -0.0;
        end if;
        if mantissa = 0 AND exponent = 255 AND sign = 0 then
            return Infinity;
        end if;
        if mantissa = 0 AND exponent = 255 AND sign = 1 then
            return -Infinity;
        end if;
        if exponent = 255 then
            return NaN;
        end if;

        result = (1 + mantissa::real / (1 << 23)) * power(2, exponent - 127);

        if sign = 1 then
            return -result;
        else
            return result;
        end if;
    end;
$$;

create or replace function pg_temp._pb_decode_double(bits bit(64)) returns double precision language plpgsql as $$
    declare
        sign int = get_bit(bits, 0);
        exponent int = substring(bits from 2 for 11);
        mantissa bigint = substring(bits from 13 for 52)::bigint;
        result double precision;
    begin
        raise debug 'sign %, mantissa %, exponent %', sign, mantissa, exponent;

        if mantissa = 0 AND exponent = 0 AND sign = 0 then
            return 0.0;
        end if;
        if mantissa = 0 AND exponent = 0 AND sign = 1 then
            return -0.0;
        end if;
        if mantissa = 0 AND exponent = 2047 AND sign = 0 then
            return Infinity;
        end if;
        if mantissa = 0 AND exponent = 2047 AND sign = 1 then
            return -Infinity;
        end if;
        if exponent = 2047 then
            return NaN;
        end if;

        result = (1 + mantissa::double precision / ((1::bigint) << 52)) * power(2, exponent - 1023);

        if sign = 1 then
            return -result;
        else
            return result;
        end if;
    end;
$$;

create type pg_temp._pb_field as (field_id bigint, type_id int, value bit varying);

-- bit varying to bytea: substring(varbit_send(bit varying) from 5)
-- bytea to bit varying: right(bytea::text, -1)::bit varying

-- gets message fields
create or replace function pg_temp._pb(payload bytea) returns setof pg_temp._pb_field language plpgsql as $$
    declare
        field_id bigint;
        type_id int;
        value bit varying;
        l int = 1;
        r int = octet_length(payload) + 1;
        spec int;
        sublen int;
        val bigint;
    begin
        loop
            if l >= r then
                exit;
            end if;

            select next_index, lpad(result::text, 32, '0')::bit(32)::int into l, spec from pg_temp._pb_parse_varint(payload, l);
            field_id = spec / 8;
            type_id = spec % 8;
            raise debug 'Found field #% of type %', field_id, type_id;

            case type_id
                when 0 then -- VARINT: int32, int64, uint32, uint64, bool, enum, sint32 (ZigZag), sint64 (ZigZag)
                    select next_index, result into l, value from pg_temp._pb_parse_varint(payload, l);
                when 1 then -- 64-BIT: fixed64, sfixed64, double
                    value = pg_temp._pb_parse_64(payload, l);
                    l = l + 8;
                when 2 then -- LENGTH PREFIXED: string, bytes, embedded messages, packed repeated fields
                    select next_index, lpad(result::text, 32, '0')::bit(32)::int into l, sublen from pg_temp._pb_parse_varint(payload, l);
                    raise debug 'before % % %', l, sublen, payload;
                    value = substring(payload::bytea from l for sublen); каким должен быть value ? bytea или bit varying ???
                    raise debug 'after';
                    l = l + sublen;
                when 3 then -- SGROUP (deprecated)
                    value = B'';
                when 4 then -- EGROUP (deprecated)
                    value = B'';
                when 5 then -- 32-BIT fixed32, sfixed32, float
                    value = pg_temp._pb_parse_32(payload, l);
                    l = l + 4;
                else
                    raise exception 'Unexpected field type %', type_id;
            end case;
            return next row(field_id, type_id, value);
        end loop;
    end;
$$;

-- gets submessage fields
create or replace function pg_temp._pb(payload bytea, variadic keys int[]) returns setof pg_temp._pb_field language plpgsql as $$
    declare
        field_id bigint;
        type_id int;
        l int = 1;
        r int = octet_length(payload) + 1;
        key_to_find_index int = 1;
        keys_len int = array_length(keys, 1);
        spec int;
        len int;
    begin
        while key_to_find_index <= keys_len loop
            raise debug 'Bytes % -> % : %', l, r, substring(payload from l for r - l);

            if l >= r then
                if key_to_find_index > 1 then
                    raise exception 'Submessage % dont have field %', array_to_string(keys[1:key_to_find_index], ' -> '), keys[key_to_find_index];
                else
                    raise exception 'Message dont have field %', keys[key_to_find_index];
                end if;
            end if;

            select next_index, lpad(result::text, 32, '0')::bit(32)::int into l, spec from pg_temp._pb_parse_varint(payload, l);
            field_id = spec / 8;
            type_id = spec % 8;
            raise debug 'Found field #% of type %', field_id, type_id;

            if field_id = keys[key_to_find_index] AND type_id != 2 then
                raise exception 'Cant get message because field % is not of message type', array_to_string(keys[1:key_to_find_index], ' -> ');
            end if;

            case type_id
                when 0 then -- VARINT
                    select next_index into l from pg_temp._pb_parse_varint(payload, l);
                when 1 then -- 64-BIT
                    l = l + 8;
                when 2 then -- VARLEN
                    select next_index, lpad(result::text, 32, '0')::bit(32)::int into l, len from pg_temp._pb_parse_varint(payload, l);
                    if field_id = keys[key_to_find_index] then
                        key_to_find_index = key_to_find_index + 1;
                        r = l + len;
                        raise debug 'Going to parse message from % with length %', l, len;
                    else
                        l = l + len;
                    end if;
                when 3 then -- SGROUP (empty payload)
                when 4 then -- EGROUP (empty payload)
                when 5 then -- 32-BIT
                    l = l + 4;
                else
                    raise exception 'Unexpected field type %', type_id;
            end case;
        end loop;

        return query select * from pg_temp._pb(substring(payload from l for r - l));
    end;
$$;

create or replace function pg_temp._visualize_fields(fields pg_temp._pb_field[]) returns table (field_id bigint, type text, value text) language plpgsql as $$
    declare
        field pg_temp._pb_field;
        r record;
    begin
        foreach field in array fields loop
            field_id = field.field_id;
            case field.type_id
                when 0 then -- VARINT: int32, int64, uint32, uint64, sint32, sint64, bool, enum
                    type = 'VARINT';
                    value = field.value::bigint::text;
                when 1 then -- 64-BIT: fixed64, sfixed64, double
                    type = '64-BIT';
                    value = field.value::bigint::text;
                when 2 then -- LENGTH PREFIXED: string, bytes, embedded messages, packed repeated fields
                    type = 'BYTES';
                    value = to_hex(field.value);
                    begin
                        type = 'TEXT';
                        value = convert_from(value::bytea, 'UTF-8');
                    exception when others then
                        -- swallow exception if conversion to text fails
                        -- raise debug 'Not UTF-8: % -> %', SQLSTATE, SQLERRM;
                    end;
                when 3 then -- SGROUP (deprecated)
                    type = 'SGROUP';
                    value = '';
                when 4 then -- EGROUP (deprecated)
                    type = 'EGROUP';
                    value = B'';
                when 5 then -- 32-BIT fixed32, sfixed32, float
                    type = '32-BIT';
                    value = field.value::bigint::text;
                else
                    raise exception 'Unexpected field type %', type_id;
            end case;
            return next;
        end loop;
    end;
$$;

create or replace function pg_temp._visualize_type(type_id int) returns text language plpgsql as $$
    begin
        case type_id
            when 0 then -- VARINT: int32, int64, uint32, uint64, sint32, sint64, bool, enum
                return 'VARINT';
            when 1 then -- 64-BIT: fixed64, sfixed64, double
                return '64-BIT';
            when 2 then -- LENGTH PREFIXED: string, bytes, embedded messages, packed repeated fields
                return 'BYTES';
            when 3 then -- SGROUP (deprecated)
                return 'SGROUP';
            when 4 then -- EGROUP (deprecated)
                return 'EGROUP';
            when 5 then -- 32-BIT fixed32, sfixed32, float
                return '32-BIT';
            else
                return 'Unexpected field type #' || type_id;
        end case;
    end;
$$;

-- returns message fields
create or replace function pg_temp.pb(payload bytea) returns table (field_id bigint, type text, value text) language plpgsql as $$
    begin
        return query select * from pg_temp._visualize_fields(pg_temp._pb(payload));
    end;
$$;

-- returns submessage fields
create or replace function pg_temp.pb(payload bytea, variadic keys int[]) returns table (field_id bigint, type text, value text) language plpgsql as $$
    begin
        return query select * from pg_temp._visualize_fields(pg_temp._pb(payload, variadic keys));
    end;
$$;

-- returns integer field
-- select pg_temp.pb_int_strict('\x080212340a160a013012111a0f0a0d080110a70118caa4b1012083020a1a0a0131121512130a111a0f0a0d080110a60118d79c9001208202', 2, 1, 2, 3, 1, 2);
create or replace function pg_temp.pb_int_strict(payload bytea, variadic keys int[]) returns bigint language plpgsql as $$
    declare
        _type text;
        _type_id int;
        _value text;
        keys_len int = array_length(keys, 1);
    begin
        if keys_len = 1 then
            select type_id, value into _type_id, _value from pg_temp._pb(payload) where field_id = keys[1];
        else
            select type_id, value into _type_id, _value from pg_temp._pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 0 then
            raise exception 'Field % is not VARINT, it is %', array_to_string(keys, ' -> '), pg_temp._visualize_type(_type_id);
        end if;
            return _value::bigint;
    end;
$$;

-- returns integer field or NULL if it is missing or has wrong type
create or replace function pg_temp.pb_int(payload bytea, variadic keys int[]) returns bigint language plpgsql as $$
    begin
        begin
            return pg_temp.pb_int_strict(payload, variadic keys);
        exception when others then
            return null;
        end;
    end;
$$;

-- returns float field
-- select pg_temp.pb_float_strict('\x0D77CCABB5', 1);
create or replace function pg_temp.pb_float_strict(payload bytea, variadic keys int[]) returns real language plpgsql as $$
    declare
        _type text;
        _type_id int;
        _value text;
        keys_len int = array_length(keys, 1);
    begin
        if keys_len = 1 then
            select type_id, value into _type_id, _value from pg_temp._pb(payload) where field_id = keys[1];
        else
            select type_id, value into _type_id, _value from pg_temp._pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 5 then
            raise exception 'Field % is not INT32, it is %', array_to_string(keys, ' -> '), pg_temp._visualize_type(_type_id);
        end if;
        return _value::float;
    end;
$$;

-- returns float field or NULL if it is missing or has wrong type
create or replace function pg_temp.pb_float(payload bytea, variadic keys int[]) returns real language plpgsql as $$
    begin
        begin
            return pg_temp.pb_float_strict(payload, variadic keys);
        exception when others then
            return null;
        end;
    end;
$$;

-- returns double field
-- select pg_temp.pb_double_strict('\x099A9999999999F13F', 1);
create or replace function pg_temp.pb_double_strict(payload bytea, variadic keys int[]) returns double precision language plpgsql as $$
    declare
        _type text;
        _type_id int;
        _value text;
        keys_len int = array_length(keys, 1);
    begin
        if keys_len = 1 then
            select type_id, value into _type_id, _value from pg_temp._pb(payload) where field_id = keys[1];
        else
            select type_id, value into _type_id, _value from pg_temp._pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 1 then
            raise exception 'Field % is not INT64, it is %', array_to_string(keys, ' -> '), pg_temp._visualize_type(_type_id);
        end if;
        return _value::double precision;
    end;
$$;

-- returns double field or NULL if it is missing or has wrong type
create or replace function pg_temp.pb_double(payload bytea, variadic keys int[]) returns double precision language plpgsql as $$
    begin
        begin
            return pg_temp.pb_double_strict(payload, variadic keys);
        exception when others then
            return null;
        end;
    end;
$$;

-- returns text field
-- select pg_temp.pb_text_strict('\x080212340a160a013012111a0f0a0d080110a70118caa4b1012083020a1a0a0131121512130a111a0f0a0d080110a60118d79c9001208202', 2, 1, 1);
create or replace function pg_temp.pb_text_strict(payload bytea, variadic keys int[]) returns text language plpgsql as $$
    declare
        _type text;
        _type_id int;
        _value text;
        keys_len int = array_length(keys, 1);
    begin
        if keys_len = 1 then
            select type_id, value into _type_id, _value from pg_temp._pb(payload) where field_id = keys[1];
        else
            select type_id, value into _type_id, _value from pg_temp._pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 2 or _type != 'TEXT' then
            raise exception 'Field % is not TEXT, it is %', array_to_string(keys, ' -> '), pg_temp._visualize_type(_type_id);
        end if;
        return _value;
    end;
$$;

-- returns text field or NULL if it is missing or has wrong type
create or replace function pg_temp.pb_text(payload bytea, variadic keys int[]) returns text language plpgsql as $$
    begin
        begin
            return pg_temp.pb_text_strict(payload, variadic keys);
        exception when others then
            return null;
        end;
    end;
$$;

-- returns boolean field
create or replace function pg_temp.pb_bool_strict(payload bytea, variadic keys int[]) returns boolean language plpgsql as $$
    declare
        _type text;
        _type_id int;
        _value text;
        keys_len int = array_length(keys, 1);
    begin
        if keys_len = 1 then
            select type_id, value into _type_id, _value from pg_temp._pb(payload) where field_id = keys[1];
        else
            select type_id, value into _type_id, _value from pg_temp._pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 0 then
            raise exception 'Field % is not int32 (BOOLEAN), it is %', array_to_string(keys, ' -> '), pg_temp._visualize_type(_type_id);
        end if;
        return _value != '0';
    end;
$$;

-- returns boolean field or NULL if it is missing or has wrong type
create or replace function pg_temp.pb_bool(payload bytea, variadic keys int[]) returns boolean language plpgsql as $$
    begin
        begin
            return pg_temp.pb_bool_strict(payload, variadic keys);
        exception when others then
            return null;
        end;
    end;
$$;

\set QUIET off

\echo 'Functions in `pg_temp` schema added: pb(), pb_int(), pb_int_strict(), pb_text(), pb_text_strict(), pb_bool(), pb_bool_strict(). All with same arguments (protobuf_message::bytea [, proto_fields_indexes::int])'