CREATE TABLE public.prices (
    id int,
    value float8,
    at date,
    PRIMARY KEY (id, at)
) PARTITION BY RANGE (at);

CREATE TABLE public.prices_2022 PARTITION OF public.prices FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE public.prices_2023 PARTITION OF public.prices FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');