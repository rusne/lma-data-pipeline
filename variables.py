# a set of variables as analysis input

# columns to extract from original LMA data
LMA_columns = ['Afvalstroomnummer', 'VerwerkingsmethodeCode',
               'VerwerkingsOmschrijving', 'RouteInzameling',
               'Inzamelaarsregeling', 'ToegestaanbijInzamelaarsregeling',
               'EuralCode', 'BenamingAfval', 'MeldPeriodeJAAR',
               'MeldPeriodeMAAND', 'Gewicht_KG', 'Aantal_vrachten',
               # Ontdoener
               'Ontdoener', 'Ontdoener_Postcode', 'Ontdoener_Plaats',
               'Ontdoener_Straat', 'Ontdoener_Huisnr',
               # Herkomst
               'Herkomst_Postcode', 'Herkomst_Straat', 'Herkomst_Plaats',
               'Herkomst_Huisnr',
               # # Afzender
               # 'Afzender', 'Afzender_Postcode', 'Afzender_Straat',
               # 'Afzender_Plaats', 'Afzender_Huisnummer',
               # # Inzamelaar
               # 'Inzamelaar', 'Inzamelaar_Postcode', 'Inzamelaar_Straat',
               # 'Inzamelaar_Plaats', 'Inzamelaar_Huisnr',
               # # Bemiddelaar
               # 'Bemiddelaar', 'Bemiddelaar_Postcode', 'Bemiddelaar_Straat',
               # 'Bemiddelaar_Plaats', 'Bemiddelaar_Huisnr',
               # # Handelaar
               # 'Handelaar', 'Handelaar_Postcode', 'Handelaar_Straat',
               # 'Handelaar_Plaats', 'Handelaar_Huisnummer',
               # # Ontvanger
               # 'Ontvanger', 'Ontvanger_Postcode', 'Ontvanger_Straat',
               # 'Ontvanger_Plaats', 'Ontvanger_Huisnummer',
               # Verwerker
               'Verwerker', 'Verwerker_Postcode', 'Verwerker_Straat',
               'Verwerker_Plaats', 'Verwerker_Huisnr']

# roles to process in LMA data
roles = ['Ontdoener', 'Herkomst', 'Verwerker']

# scopes = ['FW', 'CG', 'CDW']
#
# titles = {'FW': 'Organic Waste',
#           'CG': 'Consumption Goods',
#           'CDW': 'Construction & Demolition Waste'}
#
# titles_NL = {'FW': 'organische afvalmaterialen',
#              'CG': 'consumptie-afvalmaterialen',
#              'CDW': 'bouw- en sloopafvalmaterialen'}
#
# all_roles = ['Ontdoener', 'Herkomst', 'Afzender', 'Inzamelaar', 'Bemiddelaar',
#              'Handelaar', 'Ontvanger', 'Verwerker']
#
# map_roles = ['Ontdoener', 'Herkomst', 'Verwerker']
#
# all_years = [2013, 2014, 2015, 2016, 2017, 2018]
#
# map_years = [2013, 2014, 2015, 2016, 2017, 2018]
#
# LISA_years = [2014, 2015, 2016, 2017, 2018]
#
# buffer_dist = 250