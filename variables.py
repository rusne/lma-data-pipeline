# a set of variables as analysis input

dummy_nace = {
    "Afzender": "3810",
    "Inzamelaar": "3810",
    "Bemiddelaar": "3810",
    "Handelaar": "3810",
    "Ontvanger": "3820",
    "Verwerker": "3820"
}

# buffer distance for matching actors with KvK data
buffer_dist = 250


# Ontvangstmeldingen
roles = [
    "Ontdoener",
    "Herkomst",
    "Verwerker"
]

connect_nace = "Ontdoener"

LMA_columns = [
    "Afvalstroomnummer",
    "VerwerkingsmethodeCode",
    "VerwerkingsOmschrijving",
    "RouteInzameling",
    "Inzamelaarsregeling",
    "ToegestaanbijInzamelaarsregeling",
    "EuralCode",
    "BenamingAfval",
    "MeldPeriodeJAAR",
    "MeldPeriodeMAAND",
    "Gewicht_KG",
    "Aantal_vrachten",
    # Ontdoener
    "Ontdoener",
    "Ontdoener_Postcode",
    "Ontdoener_Plaats",
    "Ontdoener_Straat",
    "Ontdoener_Huisnr",
    # Herkomst
    "Herkomst_Postcode",
    "Herkomst_Straat",
    "Herkomst_Plaats",
    "Herkomst_Huisnr",
    # Verwerker
    "Verwerker",
    "Verwerker_Postcode",
    "Verwerker_Straat",
    "Verwerker_Plaats",
    "Verwerker_Huisnr"
]


# # Afgiftemeldingen
# roles = [
#     "Ontdoener",
#     "EerstAfnemer",
#     # "Verwerker"
# ]
#
# connect_nace = "EerstAfnemer"
#
# LMA_columns = [
#     "Verwerkersnummer",
#     "GangbareNaam",
#     # Verwerker
#     # "Verwerker_Straat",
#     # "Verwerker_Huisnummer",
#     # "Verwerker_Postcode",
#     # "Verwerker_Plaats",
#     # Ontdoener
#     "Ontdoener_Bedrijfsnr",
#     "Ontdoener",
#     "Ontdoener_Straat",
#     "Ontdoener_Huisnr",
#     "Ontdoener_Postcode",
#     "Ontdoener_Plaats",
#     "Ontdoener_Land",
#     # EerstAfnemer
#     "EerstAfnemer_Bedrijfsnr",
#     "EerstAfnemer",
#     "EerstAfnemer_Straat",
#     "EerstAfnemer_Huisnr",
#     "EerstAfnemer_Postcode",
#     "EerstAfnemer_Plaats",
#     "EerstAfnemer_Land",
#     "VerwerkingsmethodeCode",
#     "VerwerkingsOmschrijving",
#     "EuralCode",
#     "EuralcodeOmschrijving",
#     "MeldPeriodeJAAR",
#     "MeldPeriodeMAAND",
#     "Gewicht_KG",
#     "Aantal_vrachten"
# ]
