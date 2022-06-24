#!/usr/bin/env python3
from ftplib import FTP
import os
import pandas as pd
from datetime import datetime
import dropbox
import numpy as np
from sqlalchemy import create_engine

from sqlalchemy.engine.url import URL
import configparser
from pathlib import Path
import sys
sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))

date_now = datetime.now().strftime("%c").replace(":", "-")

def get_latest_file():
    with FTP(host=alg_config.get("schuurman ftp", "server")) as ftp:
        ftp.login(user=alg_config.get("schuurman ftp", "user"), passwd=alg_config.get("schuurman ftp", "passwd"))
        # ftp.retrlines('LIST')

        names = ftp.nlst()
        final_names = [line for line in names if "KSCE_" in line]

        latest_time = None
        latest_name = None

        for name in final_names:
            time = ftp.sendcmd("MDTM " + name)
            if (latest_time is None) or (time > latest_time):
                latest_name = name
                latest_time = time

        with open(latest_name, "wb") as f:
            ftp.retrbinary("RETR " + latest_name, f.write)


get_latest_file()

schuurman = pd.read_csv(
    max(Path.cwd().glob("KSCE_*.csv"), key=os.path.getctime),
    sep="\t",
    encoding="cp1250",
    header=1,
    dtype={"Artikelnr": object},
)

schuurman = (
    schuurman.rename(
        columns={
            "Artikelnr": "sku",
            "Ean": "ean",
            "Voorraad": "stock",
            "Merk": "brand",
            "Adv.prijs (incl.BTW)": "price_advice",
            "Goingprijs (incl.BTW)": "price_going",
            "Omschrijving": "info",
            "Opmerking": "note",
            "Artikelnaam": "group",
            "Type": "id",
        }
    )
    .assign(ean = lambda x: pd.to_numeric(x["ean"], errors="coerce"))
    .query("stock > 0")
    .query("ean == ean")
    .assign(
        price=lambda x: np.round(
            x["Netto (excl.BTW)"]
            .add(x["VWB bedrag"], fill_value=0)
            .add(x["BAT bedrag"], fill_value=0)
            .add(x["ATR bedrag"], fill_value=0)
            ,2),
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
        eigen_sku = lambda x:"ALR" + x["sku"],
        advies_prijs = "",
        gewicht = "",
        url_plaatje = "",
        url_artikel = "",
        lange_omschrijving = "",
        verpakings_eenheid = "",
    ).assign(price = lambda x: (x["price"] - x["lk"]).round(2))
)

schuurman_basic = schuurman[
    [
        "sku",
        "ean",
        "brand",
        "stock",
        "price",
        "price_advice",
        "price_going",
        "info",
        "note",
        "group",
        "id",
        "lk",
    ]
]

schuurman_basic.to_csv("ALR_" + date_now + ".csv", index=False, encoding="utf-8-sig")

latest_file = max(Path.cwd().glob("ALR_*.csv"), key=os.path.getctime)
with open(latest_file, "rb") as f:
    dbx.files_upload(
        f.read(),
        "/macro/datafiles/ALR/" + latest_file.name,
        mode=dropbox.files.WriteMode("overwrite", None),
        mute=True,
    )

schuurman_info = schuurman.rename(
    columns={"price": "prijs", "brand": "merk", "group": "category", "info": "product_title","stock":"voorraad"}
)
schuurman_info_db = schuurman_info[
    [
        "eigen_sku",
        "sku",
        "ean",
        "voorraad",
        "merk",
        "prijs",
        "advies_prijs",
        "category",
        "gewicht",
        "url_plaatje",
        "url_artikel",
        "product_title",
        "lange_omschrijving",
        "verpakings_eenheid",
    ]
]

current_folder = Path.cwd().name.upper()
huidige_datum = datetime.now().strftime("%d_%b_%Y")
schuurman_info_db.to_sql(f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000)

with engine.connect() as con:
    con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
    aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
    totaal_stock = int(con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    totaal_prijs = int(con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    leverancier = f"{current_folder}"
    sql_insert = (
        "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
    )
    con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

engine.dispose()