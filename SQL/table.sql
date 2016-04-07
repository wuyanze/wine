use wine
create table init(
    id int(7) not null,
    name varchar(100),
    country varchar(100),
    type char(30),
    maker varchar(100),
    region varchar(50),
    color char(10),
    capacity char(25),
    grape char(30),
    primary key(id)
);