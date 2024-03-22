#!/usr/bin/env python3
from ftplib import FTP
import os
import pandas as pd
from datetime import datetime
import numpy as np
import configparser
from pathlib import Path
import sys

sys.path.insert(0, str(Path.cwd().parent))
from bol_export_file import get_file
from process_results.process_data import save_to_db, save_to_dropbox, save_to_dropbox_vendit

ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")
scraper_name = Path.cwd().name
korting_percent = int(ini_config.get("stap 1 vaste korting", scraper_name.lower()).strip("%"))

date_now = datetime.now().strftime("%c").replace(":", "-")


def get_latest_file():
    with FTP(host=ini_config.get("schuurman ftp", "server")) as ftp:
        ftp.login(user=ini_config.get("schuurman ftp", "user"), passwd=ini_config.get("schuurman ftp", "passwd"))
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

vooraad_info = (
    pd.read_csv(
        max(Path.cwd().glob("KSCE_*.csv"), key=os.path.getctime),
        sep="\t",
        encoding="cp1250",
        header=1,
        dtype={"Artikelnr": object},
    )
    .rename(
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
    .assign(ean=lambda x: pd.to_numeric(x["ean"], errors="coerce"))
    .query("stock > 0")
    .query("ean == ean")
    .assign(
        price=lambda x: np.round(
            x["Netto (excl.BTW)"]
            .add(x["VWB bedrag"], fill_value=0)
            .add(x["BAT bedrag"], fill_value=0)
            .add(x["ATR bedrag"], fill_value=0),
            2,
        ),
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
        eigen_sku=lambda x: scraper_name + x["sku"],
    )
    .assign(price=lambda x: (x["price"] - x["lk"]).round(2))
)

vooraad_info = vooraad_info[
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

vooraad_info.to_csv(f"{scraper_name}_{date_now}.csv", index=False, encoding="utf-8-sig")

latest_file = max(Path.cwd().glob(f"{scraper_name}_*.csv"), key=os.path.getctime)
save_to_dropbox(latest_file, scraper_name)

extra_columns = {"BTW code": 21, "Leverancier": scraper_name.lower()}
vendit = vooraad_info.assign(**extra_columns, ean=lambda x: x.ean.astype("string").str.split(".").str[0]).rename(
    columns={
        "eigen_sku": "Product nummer",
        "ean": "EAN nummer",
        "price": "Inkoopprijs exclusief",
        "brand": "Merk",
        "price_advice": "Verkoopprijs inclusief",
        "group": "Groep Niveau 1",
        "info": "Product omschrijving",
    }
)

save_to_dropbox_vendit(vendit, scraper_name)

product_info = vooraad_info.rename(
    columns={
        # "sku":"onze_sku",
        # "ean":"ean",
        "brand": "merk",
        "stock": "voorraad",
        "price": "inkoop_prijs",
        # :"promo_inkoop_prijs",
        # :"promo_inkoop_actief",
        "group" :"category",
        "price_advice": "advies_prijs",
        "info": "omschrijving",
    }
).assign(onze_sku=lambda x: scraper_name + x["sku"], import_date=datetime.now())

save_to_db(product_info)
