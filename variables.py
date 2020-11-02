# a set of variables as analysis input

# roles to process in LMA data
roles = [
    "Ontdoener",
    "Herkomst",
    "Verwerker"
]

# columns to extract from original LMA data
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
