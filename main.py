import csv
import sqlite3
import os

# function for processing the data geolocation
def dim_geog(geo_data):
    dict_geo_data = {}
    with open('csv/dim_geog.csv') as data:
        csv_reader = csv.reader(data, delimiter=';')
        #print("======List Geolocation=====\n")
        for row in csv_reader:
            if "lvl_id" not in row:
                if geo_data['lvl1'].lower() == row[1].lower():
                    dict_geo_data['lvl1'] = row[2].lower()
                elif geo_data['lvl2'].lower() == row[1].lower():
                    dict_geo_data['lvl2'] = row[2].lower()
                elif geo_data['lvl3'].lower() == row[1].lower():
                    dict_geo_data['lvl3'] = row[2].lower()
    return dict_geo_data

# function for proccessing the data lookup geolocation
def dim_lookup_geog(lookup_geog):
    lookup_geo_loc = {}
    with open('csv/dim_lookup_geog.csv') as data:
        csv_reader = csv.reader(data, delimiter=';')
        #print("\n======List Lookup Geolocation=====")
        for row in csv_reader:
            if "country_id" not in row and row[1].lower() == lookup_geog.lower():
                lookup_geo_loc["lvl1"] = row[2]
                lookup_geo_loc["lvl2"] = row[3]
                lookup_geo_loc["lvl3"] = row[4]
                return lookup_geo_loc
    return lookup_geo_loc


#function for reading the country
def dim_country(country):
    with open('csv/dim_country.csv') as data:
        csv_reader = csv.reader(data, delimiter=';')
        #print("\n======List Country=====")
        for row in csv_reader:
            if "country_id" not in row and row[2].lower()== country.lower():
                return row[1]
    return ""


def init_db(db_file):
    con  = None
    try:
        # initiate db and table
        con = sqlite3.connect(db_file)
        cur = con.cursor()
        cur.execute("""CREATE TABLE "dim_stores" (
	        "store_id"	INTEGER,
	        "store_code"	TEXT,
	        "store_name"	TEXT,
	        "country_code"	TEXT,
	        "street_name"	TEXT,
	        "pin_code"	TEXT,
	        "lvl1_geog"	TEXT,
	        "lvl2_geog"	TEXT,
	        "lvl3_geog"	TEXT,
            "automatable" TEXT,
	        PRIMARY KEY("store_id" AUTOINCREMENT)
        );""");

        # seeding data in row 1 and 2 as pre-exsiting data
        cur.execute("""
            INSERT INTO dim_stores
                (
                    store_code, store_name, country_code,
                    street_name, pin_code, lvl1_geog,
                    lvl2_geog, lvl3_geog, automatable
                ) VALUES
            ('IND1', '7-11-store1', 'IND', 'thomas street', '600098', 'Tamil Nadu', 'Cenai', '', 'N'),
            ('ID2', 'Test Store Jakarta', 'ID', 'Jl Durian 19, Sumatera Utara', '20235', 'Medan', 'Sumatera Utara', '', 'N')
        """);
        con.commit()

    except Exception as e:
        print(e)
    finally:
        if con:
            con.close()

# getting last index db
def get_last_insert_row(db_file):
    idx = -1
    con  = None
    try:
        # open connection database
        con = sqlite3.connect(db_file)
        cur = con.cursor()

        # get last row inserted sqlite
        cur.execute("""
           select store_id from dim_stores order by store_id desc limit 1;
        """);
        idx = cur.fetchone()[0]
    except Exception as e:
        print(e)
    finally:
        if con:
            con.close()
    return idx

def insert_store_db(list_dim_store, db_file):
    con  = None
    try:
        # initiate db and table
        con = sqlite3.connect(db_file)
        cur = con.cursor()
        list_store_inserted = []
        for store in list_dim_store:
            temp_store  = "('"+ store['store_code']+"','" +store['store_name']+"','"+store['country_code']+"','"+store['street_name']+"','"+store['pin_code']+"','"+store['lvl1_geog']+"','"+store['lvl2_geog']+"','"+store['lvl3_geog']+"','Y')"
            list_store_inserted.append(temp_store)

        # insert_sample_data
        temp_sql_query = "INSERT INTO dim_stores( store_code, store_name, country_code, street_name, pin_code, lvl1_geog, lvl2_geog, lvl3_geog, automatable) VALUES "+",".join(list_store_inserted)
        cur.execute(temp_sql_query);
        con.commit()

        print("====================================================")
        print("=            Successfully ETL Process              =")
        print("=  all the result could be saw in db/dim_store.db  =")
        print("=             using sqlite browser                =")
        print("====================================================")
    except Exception as e:
        print(e)
    finally:
        if con:
            con.close()


def seed_sample_data():
    list_dim_country_data = []
    idx = get_last_insert_row("db/dim_store.db")
    with open('csv/Sample csv data.csv') as data:
        csv_reader = csv.reader(data, delimiter=';')
        #print("\n======List Smple Data Seed=====")
        for row in csv_reader:
                if "country_name" not in row and "Include exact door number" not in row:
                    idx+=1
                    country = row[1]
                    #print(country)
                    country_code = dim_country(country)
                    lookup_geog = dim_lookup_geog(country_code)
                    geog_data = dim_geog(lookup_geog)
                    temp_store = {}
                    temp_store['store_code'] = country_code+str(idx)
                    temp_store['store_name'] = row[0]
                    temp_store['country_code'] = country_code
                    temp_store['street_name'] = row[2]
                    temp_store['pin_code'] = row[3]
                    temp_store['lvl1_geog'] = geog_data['lvl1'] if 'lvl1' in geog_data else ''
                    temp_store['lvl2_geog'] = geog_data['lvl2'] if 'lvl2' in geog_data else ''
                    temp_store['lvl3_geog'] = geog_data['lvl3'] if 'lvl3' in geog_data else ''
                    list_dim_country_data.append(temp_store)

    insert_store_db(list_dim_country_data, "db/dim_store.db")

if __name__ == "__main__":
    if not os.path.exists("db/dim_store.db"):
        init_db("db/dim_store.db")
    seed_sample_data()

