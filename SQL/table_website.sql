use wine
create table wineweb(
    url varchar(150) not null,
    primary key(url)
);
create table winedata(
    productId char(10) not null,
    pproductId varchar(30),
    title varchar(100),
    region varchar(50),
    varietalId char(10),
    vineyardId char(10),
    productType char(10),
    price char(15),
    primary key(productId)
);
create table winereview(
    productId char(10) not null,
    username varchar(30) not null,
    ratingvalue char(5),
    primary key(productId,username)
);