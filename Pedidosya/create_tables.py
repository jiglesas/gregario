import psycopg2
from psycopg2 import Error


#Security check
Xcommands = (
    """
    DROP TABLE IF EXISTS pedidosyacl_products
    """,
    """
    DROP TABLE IF EXISTS pedidosyacl_partners
    """,
    """
    DROP TABLE IF EXISTS pedidosyacl_sections
    """,)

commands = (
    """
    CREATE TABLE pedidosyacl_partners (
        partner_id VARCHAR(255) PRIMARY KEY,
        business_type VARCHAR(255),
        name TEXT,
        address TEXT,
        url TEXT,
        description TEXT,
        sections TEXT,
        tags TEXT,
        active BOOLEAN NOT NULL DEFAULT true,
        create_date TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE pedidosyacl_sections (
            section_id VARCHAR(255) PRIMARY KEY,
            name TEXT,
            index VARCHAR(255),
            active BOOLEAN NOT NULL DEFAULT true,
            create_date TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE pedidosyacl_products (
            product_id VARCHAR(255),
            name TEXT,
            price VARCHAR(255),
            section_id VARCHAR(255),
            partner_id VARCHAR(255),
            create_date TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """)


try:
    # Connect to an existing database
    conn = psycopg2.connect(user="mgandolfi",
                                  password="mgandolfi2021",
                                  host="34.69.175.136",
                                  port="5432",
                                  database="pimvault",
                                  options="-c search_path=dbo,scrapinghub")


    cur = conn.cursor()
    cur.execute('''SELECT * FROM pedidosyacl_partners LIMIT 100''')
    print(cur.fetchall())
    
    #Security check
    import ipdb; ipdb.set_trace()
    # # create table one by one
    for command in commands:
        cur.execute(command)
        conn.commit()
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()





