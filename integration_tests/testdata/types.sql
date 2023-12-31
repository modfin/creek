CREATE TABLE public.types
(
    bool           bool,
    char           char,
    varchar        varchar,
    bpchar         bpchar,
    date           date,
    float4         float4,
    float8         float8,
    int2           int2,
    int4           int4,
    int8           int8,
    json           json,
    jsonb          jsonb,
    text           text,
    time           time,
    timestamp      timestamp,
    timestamptz    timestamptz,
    uuid           uuid PRIMARY KEY,
    numeric        numeric(10,5),
    boolArr        bool[],
    charArr        char[],
    varcharArr     varchar[],
    bpcharArr      bpchar[],
    dateArr        date[],
    float4Arr      float4[],
    float8Arr      float8[],
    int2Arr        int2[],
    int4Arr        int4[],
    int8Arr        int8[],
    jsonArr        json[],
    jsonbArr       jsonb[],
    textArr        text[],
    timeArr        time[],
    timestampArr   timestamp[],
    timestamptzArr timestamptz[],
    uuidArr        uuid[],
    numericArr     numeric(10,5)[]
);

