-- https://protobuf.dev/programming-guides/encoding/
-- NOTICE FOR DEVELOPRS:
--   function substring(bytea, left, length);
--     left index counts from 1 in bytes
--   function get_byte(bytea, index);
--     index counts from 0 byte

--TODO
-- 1. create or replace function pb_float(payload bytea) returns real language plpgsql as $$
-- 2. create or replace function pb_double(payload bytea) returns double language plpgsql as $$
-- solution in https://stackoverflow.com/questions/9374561/how-to-cast-type-bytea-to-double-precision/11661849#11661849
-- 3. support repeated fields



-- to see debug messages
-- set client_min_messages = 'DEBUG1';
\set QUIET on

-- select pg_temp._pb_parse_varint('\x0a2462383861663337662d626530642d343037382d386237322d333037653834363736343939', 1);
-- from_index and next_index counts from 1 byte
create or replace function pg_temp._pb_parse_varint(pb bytea, from_index int) returns table (next_index int, result bigint) language plpgsql as $$
    declare
        b int;
        len int = octet_length(pb);
    begin
        next_index = from_index;
        result = 0;
        loop
            exit when next_index > len;

            b = get_byte(pb, next_index - 1);
            result = result + (b % 128) * (128 ^ (next_index - from_index));
            next_index = next_index + 1;

            exit when b < 128;
            if next_index > len then
                raise exception 'Failed to parse varint: byte sequence didnt end with trailing byte with 0 in most significant bit.';
            end if;
            if next_index - from_index > 10 then -- invalid varint
                raise exception 'Failed to parse varint: byte sequence cant be longer than 10.';
            end if;
        end loop;
        return next;
    end;
$$;


create or replace function pg_temp._pb_num(payload bytea) returns bigint language plpgsql as $$
    declare
        len int = octet_length(payload) - 1;
        result bigint = 0;
    begin
        if len > 7 then
            raise exception 'Number cant be longer than 8 bytes, but it is % bytes', len;
        end if;
        while len >=0 loop
            result = result * 256 + get_byte(payload, len);
            len = len - 1;
        end loop;
        return result;
    end;
$$;

-- select pg_temp.pb('\x0a2462383861663337662d626530642d343037382d386237322d333037653834363736343939');
-- select pg_temp.pb('\x080212340a160a013012111a0f0a0d080110a70118caa4b1012083020a1a0a0131121512130a111a0f0a0d080110a60118d79c9001208202');

-- shows message fields
create or replace function pg_temp.pb(payload bytea) returns table (field_id bigint, type_id int, type text, value text) language plpgsql as $$
    declare
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

            select next_index, result into l, spec from pg_temp._pb_parse_varint(payload, l);
            field_id = spec / 8;
            type_id = spec % 8;
            raise debug 'Found field #% of type %', field_id, type_id;

            case type_id
                when 0 then -- VARINT: int32, int64, uint32, uint64, sint32, sint64, bool, enum
                    type = 'VARINT';
                    select next_index, result into l, val from pg_temp._pb_parse_varint(payload, l);
                    value = val;
                when 1 then -- 64-BIT: fixed64, sfixed64, double
                    type = '64-BIT';
                    value = pg_temp._pb_num(substr(payload, l, 8));
                    l = l + 8;
                when 2 then -- LENGTH PREFIXED: string, bytes, embedded messages, packed repeated fields
                    type = 'BYTES';
                    select next_index, result into l, sublen from pg_temp._pb_parse_varint(payload, l);
                    value = substr(payload, l, sublen);
                    begin
                        value = convert_from(value::bytea, 'UTF-8');
                        type = 'TEXT';
                    exception when others then
                        -- swallow exception if conversion to text fails
                        -- raise debug 'Not UTF-8: % -> %', SQLSTATE, SQLERRM;
                    end;
                    l = l + sublen;
                when 3 then -- SGROUP (deprecated)
                    type = 'SGROUP';
                    value = '';
                    return next;
                when 4 then -- EGROUP (deprecated)
                    type = 'EGROUP';
                    value = '';
                    return next;
                when 5 then -- 32-BIT fixed32, sfixed32, float
                    type = '32-BIT';
                    value = pg_temp._pb_num(substr(payload, l, 4));
                    l = l + 4;
                else        -- LEGACY: SGROUP (group start), EGROUP (group end) OR something new
                    raise exception 'Unexpected field type %', type_id;
            end case;
            return next;
        end loop;
    end;
$$;


-- shows submessage fields
-- select pg_temp.pb('\x080212340a160a013012111a0f0a0d080110a70118caa4b1012083020a1a0a0131121512130a111a0f0a0d080110a60118d79c9001208202', 2);
create or replace function pg_temp.pb(payload bytea, variadic keys int[]) returns table (field_id bigint, type_id int, type text, value text) language plpgsql as $$
    declare
        type_id int;
        l int = 1;
        r int = octet_length(payload) + 1;
        key_to_find_index int = 1;
        keys_len int = array_length(keys, 1);
        spec int;
        len int;
    begin
        while key_to_find_index <= keys_len loop
            raise debug 'Bytes % -> % : %', l, r, substr(payload, l, r - l);

            if l >= r then
                if key_to_find_index > 1 then
                    raise exception 'Submessage % dont have field %', array_to_string(keys[1:key_to_find_index], ' -> '), keys[key_to_find_index];
                else
                    raise exception 'Message dont have field %', keys[key_to_find_index];
                end if;
            end if;

            select next_index, result into l, spec from pg_temp._pb_parse_varint(payload, l);
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
                    select next_index, result into l, len from pg_temp._pb_parse_varint(payload, l);
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

        return query select * from pg_temp.pb(substr(payload, l, r - l));
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
            select type_id, type, value into _type_id, _type, _value from pg_temp.pb(payload) where field_id = keys[1];
        else
            select type_id, type, value into _type_id, _type, _value from pg_temp.pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id not in (0, 1, 5) then
            raise exception 'Field % is not NUMERIC, it is %', array_to_string(keys, ' -> '), _type;
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
            select type_id, type, value into _type_id, _type, _value from pg_temp.pb(payload) where field_id = keys[1];
        else
            select type_id, type, value into _type_id, _type, _value from pg_temp.pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 2 or _type != 'TEXT' then
            raise exception 'Field % is not TEXT, it is %', array_to_string(keys, ' -> '), _type;
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
            select type_id, type, value into _type_id, _type, _value from pg_temp.pb(payload) where field_id = keys[1];
        else
            select type_id, type, value into _type_id, _type, _value from pg_temp.pb(payload, variadic keys[1:keys_len - 1]) where field_id = keys[keys_len];
        end if;
        if _type_id != 5 then
            raise exception 'Field % is not int32 (BOOLEAN), it is %', array_to_string(keys, ' -> '), _type;
        end if;
        return _value != 0;
    end;
$$;

-- returns text field or NULL if it is missing or has wrong type
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

\echo 'Functions pg_temp.pb(), pg_temp.pb_int(), pg_temp.pb_int_strict(), pg_temp.pb_text(), pg_temp.pb_text_strict(), pg_temp.pb_bool(), pg_temp.pb_bool_strict() added, all with same arguments (protobuf_message::bytea [, proto_fields_indexes::int])'