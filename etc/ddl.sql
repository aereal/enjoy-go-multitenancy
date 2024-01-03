create database tenant_1;

use tenant_1;

create table if not exists users (
  id char(20) character set ascii primary key,
  name varchar(255) not null unique
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 collate=utf8mb4_unicode_ci;

create database tenant_2;

use tenant_2;

create table if not exists users (
  id char(20) character set ascii primary key,
  name varchar(255) not null unique
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 collate=utf8mb4_unicode_ci;

create database tenant_3;

use tenant_3;

create table if not exists users (
  id char(20) character set ascii primary key,
  name varchar(255) not null unique
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 collate=utf8mb4_unicode_ci;

create database shared;

use shared;

create table if not exists blogs (
  id char(20) character set ascii primary key,
  name varchar(255) not null unique,
  url text character set ascii
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 collate=utf8mb4_unicode_ci;
