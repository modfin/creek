create table public.test
(
    id   integer not null
        primary key,
    data text
);

alter table public.test
    owner to postgres;

