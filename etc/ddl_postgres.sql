create table events (
  event_id serial primary key,
  kind varchar(255) not null,
  message text not null,
  occurred_at timestamp with time zone not null
);
