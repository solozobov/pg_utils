-- https://protobuf.dev/programming-guides/encoding/
-- NOTICE:
--   function substring(bytea, left, length);
--     left index counts from 1 in bytes
--   function get_byte(bytea, index);
--     index counts from 0 byte


-- from_index and next_index counts from 1 byte
create or replace function pb_parse_varint(in pb bytea, in from_index int, out next_index int, out result bigint) language plpgsql as $$
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
    end;
$$;

-- select pb_parse_varint('\x0a2462383861663337662d626530642d343037382d386237322d333037653834363736343939', 1);

create or replace function pb_varint(payload bytea) returns bigint language plpgsql as $$
    declare
        res bigint;
    begin
        select result into res from pb_parse_varint(payload, 1);
        return res;
    end;
$$;

-- select pb_varint('\x12');

create or replace function pb_str(payload bytea) returns text language plpgsql as $$
    begin
        return convert_from(payload, 'UTF-8');
    end;
$$;

-- select pb_str('\x74657374696e67');

create or replace function pb_num(payload bytea) returns bigint language plpgsql as $$
    declare
        i int = octet_length(payload) - 1;
        result bigint = 0;
    begin
        if i > 7 then
            raise exception 'Number cant be longer than 8 bytes, but it is % bytes', i;
        end if;
        while i >=0 loop
            result = result * 256 + get_byte(payload, i);
            i = i - 1;
        end loop;
        return result;
    end;
$$;

--TODO
-- create or replace function pb_float(payload bytea) returns real language plpgsql as $$
-- create or replace function pb_double(payload bytea) returns double language plpgsql as $$
-- solution in https://stackoverflow.com/questions/9374561/how-to-cast-type-bytea-to-double-precision/11661849#11661849

create or replace function pb(payload bytea) returns table (field_id bigint, type text, value text) language plpgsql as $$
    declare
        l int = 1;
        r int = octet_length(payload) + 1;
        spec int;
        type_id int;
        sublen int;
        val bigint;
    begin
        loop
            if l >= r then
                exit;
            end if;

            select next_index, result into l, spec from pb_parse_varint(payload, l);
            field_id = spec / 8;
            type_id = spec % 8;
            case type_id
                when 0 then -- VARINT: int32, int64, uint32, uint64, sint32, sint64, bool, enum
                    type = 'NUMBER';
                    select next_index, result into l, val from pb_parse_varint(payload, l);
                    value = val;
                when 1 then -- 64-BIT: fixed64, sfixed64, double
                    type = '64-BIT';
                    value = pb_num(substr(payload, l, 8));
                    l = l + 8;
                when 2 then -- LENGTH PREFIXED: string, bytes, embedded messages, packed repeated fields
                    type = 'MESSAGE';
                    select next_index, result into l, sublen from pb_parse_varint(payload, l);
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
                    value = pb_num(substr(payload, l, 4));
                    l = l + 4;
                else        -- LEGACY: SGROUP (group start), EGROUP (group end) OR something new
                    raise exception 'Unexpected field type %', type_id;
            end case;
            return next;
        end loop;
    end;
$$;

-- select * from pb('\x0a2462383861663337662d626530642d343037382d386237322d333037653834363736343939');
-- select * from pb('\x080212340a160a013012111a0f0a0d080110a70118caa4b1012083020a1a0a0131121512130a111a0f0a0d080110a60118d79c9001208202');

create or replace function pb(payload bytea, variadic keys int[]) returns table (field_id bigint, type text, value text) language plpgsql as $$
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

            select next_index, result into l, spec from pb_parse_varint(payload, l);
            field_id = spec / 8;
            type_id = spec % 8;
            raise debug 'Found field #% of type %', field_id, type_id;

            case type_id
                when 0 then -- VARINT
                    select next_index into l from pb_parse_varint(payload, l);
                when 1 then -- 64-BIT
                    l = l + 8;
                when 2 then -- VARLEN
                    select next_index, result into l, len from pb_parse_varint(payload, l);
                when 3 then -- SGROUP (empty payload)
                when 4 then -- EGROUP (empty payload)
                when 5 then -- 32-BIT
                    l = l + 4;
                else
                    raise exception 'Unexpected field type %', type_id;
            end case;

            if field_id = keys[key_to_find_index] then
                key_to_find_index = key_to_find_index + 1;
                if key_to_find_index > keys_len then
                    exit;
                elseif type_id != 2 then
                    raise exception 'Cant get % value because field % is not of message type', array_to_string(keys, ' -> '), array_to_string(keys[1:key_to_find_index], ' -> ');
                else
                    r = l + len;
                end if;
                raise debug 'Going to parse message from % with length %', l, len;
            else
                if type_id = 2 then
                    l = l + len;
                end if;
            end if;
        end loop;

        case type_id
            when 0 then
                type = 'VARINT';
                value = pb_varint(substr(payload, l, len));
                return next;
            when 1 then
                type = '64-BIT';
                value = pb_num(substr(payload, l, 8));
                return next;
            when 2 then
                return query select p.field_id, p.type, p.value from pb(substr(payload, l, len)) as p;
            when 3 then -- SGROUP (empty payload)
                type = 'SGROUP';
                value = '';
                return next;
            when 4 then -- EGROUP (empty payload)
                type = 'EGROUP';
                value = '';
                return next;
            when 5 then
                type = '32-BIT';
                value = pb_num(substr(payload, l, 4));
                return next;
            else
                raise exception 'Unexpected field type %', type_id;
        end case;
    end;
$$;

-- select * from pb('\x080212340a160a013012111a0f0a0d080110a70118caa4b1012083020a1a0a0131121512130a111a0f0a0d080110a60118d79c9001208202', 2);
