use wine
create table web(
    url char(50) not null,
    primary key(url)
);
create table data(
    id int(7) not null,
    title varchar(50),
    subtitle varchar(50),
    price varchar(20),
    type varchar(30),
    country varchar(20),
    vol char(10),
    occasion varchar(50),
    grape varchar(50),
    primary key(id)
);