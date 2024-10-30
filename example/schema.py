from sssm.db_entities.declared import *

system_admin_login = Login("sa")

server = Server(
    databases=[
        Database(
            name="MyDatabase",
            owner=system_admin_login.name,
            # Can also specify schema name if desired, otherwise default "db" is used
            tables=[
                Table(
                    name="Person",
                    columns=[
                        IdentityColumn(name="ID", primary_key=True),
                        VarcharColumn(name="Name", char_max_len=255),
                        DateColumn(name="DateOfBirth"),
                        IntegerColumn(name="HeightCM"),
                        FloatColumn(name="WeightKG"),
                        IntegerColumn(name="AddressID"),
                    ],
                    indexes=[
                        Index(name="IX_MyTable_Name", columns=["Name"], compression="PAGE"),
                    ],
                    foreign_keys=[
                        ForeignKey(
                            column="AddressID",
                            foreign_table="Address",
                            foreign_column="ID",
                        ),
                    ],
                ),
                Table(
                    name="Address",
                    columns=[
                        IdentityColumn(name="ID", primary_key=True),
                        IntegerColumn(name="StreetNumber"),
                        VarcharColumn(name="StreetName", char_max_len=255),
                        VarcharColumn(name="PostCode", char_max_len=6),
                        VarcharColumn(name="State", char_max_len=100),
                        VarcharColumn(name="Country", char_max_len=255),
                    ],
                ),
            ],
            users=[
                User.for_login(system_admin_login),
            ],
        )
    ],
    logins=[
        system_admin_login,
    ],
)
