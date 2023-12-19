create table if not exists users (
  id char(20) character set ascii primary key,
  name varchar(255) not null
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 collate=utf8mb4_unicode_ci;
