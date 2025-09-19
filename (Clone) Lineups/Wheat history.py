# Databricks notebook source
from LDCDataAccessLayerPy import KeyVaultManager, SharePointManager, SqlManager, databricks_init
from datetime import datetime, timedelta
from LDCDataAccessLayerPy import databricks_init, DataLakeManagerGen2
from io import BytesIO
import LDCDataAccessLayerPy
#Initiate the secret to access KeyVault secrets
databricks_init(dbutils, 'GO')
sp_mgr = SharePointManager()
sql_mgr = SqlManager()

import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)

import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.colors import ListedColormap
from sklearn.cluster import KMeans

from datetime import datetime, timedelta
import re


# COMMAND ----------

url = "https://ldcom365.sharepoint.com"

# COMMAND ----------

# MAGIC %md
# MAGIC # Dictionnaries

# COMMAND ----------

country_region_mapping = {
    "ALGERIA": "AFRICA",
    'ALEGERIA':'AFRICA',
    "ARGENTINA": "SOUTH AMERICA",
    "BANGLADESH": "ASIA",
    "BELGIUM": "EU",
    "BRAZIL": "SOUTH AMERICA",
    "CHILE": "SOUTH AMERICA",
    "CHINA": "ASIA",
    "COLOMBIA": "SOUTH AMERICA",
    "COSTA RICA": "CENTRAL AMERICA",
    "CUBA": "CENTRAL AMERICA",
    "CYPRUS": "EU",
    "CYPRUS/GREECE": "EU",
    "DENMARK": "EU",
    "EGYPT": "AFRICA",
    "FRANCE": "EU",
    "GERMANY": "EU",
    "GREECE": "EU",
    "GREECE/ISRAEL": "EU",
    "GREECE/ITALY": "EU",
    "HOLLAND": "EU",
    "HOLLAND/GERMANY": "EU",
    "HONDURAS": "CENTRAL AMERICA",
    "INDONESIA": "SE ASIA",
    "IRAN": "ME ASIA",
    "IRELAND": "EU",
    "ISRAEL": "ME ASIA",
    "ISRAEL/GREECE": "EU",
    "ITALY": "EU",
    "IVORY COAST": "AFRICA",
    "JAPAN": "ASIA",
    "JORDAN": "ME ASIA",
    "KENYA": "AFRICA",
    'REUNION':'AFRICA',
    "KOREA": "ASIA",
    "LEBANON": "ME ASIA",
    "LITHUANIA": "EU",
    "MALAYSIA": "SE ASIA",
    "MAURITIUS": "AFRICA",
    "MEXICO": "CENTRAL AMERICA",
    "MOROCCO": "AFRICA",
    "NETHERLANDS": "EU",
    "NETHERLANDS/GERMANY": "EU",
    "NIGERIA": "AFRICA",
    "PAKISTAN": "ME ASIA",
    "PANAMA": "CENTRAL AMERICA",
    "PERU": "SOUTH AMERICA",
    "PHILIPPINES": "SE ASIA",
    "PORTUGAL": "EU",
    "PUERTO RICO": "CENTRAL AMERICA",
    "ROMANIA": "EU",
    "RUSSIA": "ASIA",
    "SAF": "AFRICA",
    "SAUDI ARABIA": "ME ASIA",
    "SENEGAL": "AFRICA",
    "SOUTH AFRICA": "AFRICA",
    "SOUTH KOREA": "ASIA",
    "SPAIN": "EU",
    "SYRIA": "ME ASIA",
    "TAIWAN": "ASIA",
    "TBC": "TBC",
    "THAILAND": "SE ASIA",
    "TUNISIA": "AFRICA",
    "TURKEY": "ASIA",
    "U.ARAB EMIRAT": "ME ASIA",
    "UAE": "ME ASIA",
    "UK": "EU",
    "UNITED KINGDOM": "EU",
    "POLAND": "EU",
    "SLOVENIA": "EU",
    "LITUANIA": "EU",
    "CROATIA": "EU",
    
    "UNITED ARAB EMIRATES": "ME ASIA",
    "URUGUAY": "SOUTH AMERICA",
    "USA": "NORTH AMERICA",
    "UNITED STATES": "NORTH AMERICA",
    "VENEZUELA": "CENTRAL AMERICA",
    "VIETNAM": "SE ASIA",
    "YEMEN": "ME ASIA",
    "": "NOT AVAILABLE",
    "MOZAMBIQUE": "AFRICA",
    "GUATEMALA": "CENTRAL AMERICA",
    "US": "NORTH AMERICA",
    "Z. OTHER EAST AFRICA": "AFRICA",
    "DOMINICAN REPUBLIC": "CENTRAL AMERICA",
    "AUSTRALIA": "OCEANIA",
    "NEW ZEALAND": "OCEANIA",
    "EL SALVADOR": "CENTRAL AMERICA",
    "NICARAGUA": "CENTRAL AMERICA",
    "OMAN": "ME ASIA",
    "KUWAIT": "ME ASIA",
    "SWITZERLAND": "EU",
    "IRAQ": "ME ASIA",
    "HAITI": "CENTRAL AMERICA",
    "CANADA": "NORTH AMERICA",
    "TRINIDAD": "CENTRAL AMERICA",
    "JAMAICA": "CENTRAL AMERICA",
    "ANGOLA": "AFRICA",
    "Z. OTHER FSU": "EUROPE",
    "NORWAY": "EU",
    "NOT AVAILABLE": "",
    "ECUADOR":"SOUTH AMERICA",
    'GUYANA':"SOUTH AMERICA",
    'TANZANIA':'AFRICA',
    'LUANDA':'AFRICA',
    'LIBYA':'AFRICA', 
    'PHILIPPINNES':'SE ASIA',
    'REUNION ISLAND':'AFRICA',
    'GHANA':'AFRICA',
    'CONGO':'AFRICA',
    'NAMIBIA':'AFRICA',
    'CAPE VERDE':'AFRICA',
    'MAURITANA':'AFRICA',
    'UGANDA':'AFRICA',
    'ZIMBABWE':'AFRICA',
    'MALI':'AFRICA',
    'SUDAN':'AFRICA',
    'MAURITANIA':'AFRICA', 
    'ETHIOPIA':'AFRICA',
    'RUANDA':'AFRICA',
    'RWANDA':'AFRICA',
    'BURUNDI':'AFRICA', 
    'LYBIA':'AFRICA',
    'MAURITUS IS':'AFRICA',
    'LATVIA':'EU',
    'ESTONIA':'EU',

    'CAMEROON':'AFRICA',
    'IVORY COST':'AFRICA',
    'DOM. REP':'CENTRAL AMERICA',
    'DOM REP.':'CENTRAL AMERICA',
    'DOM REP;':'CENTRAL AMERICA',
    'DOM. REP.':'CENTRAL AMERICA',
    'DOM. REP;':'CENTRAL AMERICA',
    'DOM REP':'CENTRAL AMERICA',
    'TRINIDAD & TOBAGO':'CENTRAL AMERICA',
    'GEORGIA':'ME ASIA',
    'KUWEIT':'ME ASIA',
    'BAHREIN':'ME ASIA',
    'DJBOUTI':'ME ASIA',
    'SAUDI ARABIA ':'ME ASIA',
    'SAUDI ARABIA + UNITED ARAB EMIRATES + OMAN':'ME ASIA',	
    
    'BRUNEI':'SE ASIA',
    'MYANMAR':'SE ASIA',
    'MALAYSIA':'SE ASIA',
    'Malaysia':'SE ASIA',
    'PHILIPINES':'SE ASIA',
    'INDIA':'ASIA',
    'BELARUS':'ASIA',
    
    'NEW ZELAND':'OCEANIA',
    'MAURITUIS':'AFRICA',
    'U.A.E.':'ME ASIA',
    'EAU':'ME ASIA',
    'LEBANNON':'ME ASIA',
    'HOLANDA':'EU',
    'PAKISTAN':'ASIA',
    'PAKISTAN ':'ASIA',
    'RUSSIAN FEDERATION':'ASIA',
    'UNITED STATES':'ASIA',
    'TURKEY ':'ME ASIA',

    'SOUTH KOREA ':'ASIA',
    'JAPAN  ':'ASIA',
    'MALAYSIA  ':'SE ASIA',
    'IRAK':'ME ASIA', 
    'BRASIL':'SOUTH AMERICA',
    'MARRUECOS':'AFRICA',
    'ECUADOR ':'SOUTH AMERICA',
    'PARAGUAY':'SOUTH AMERICA',
    'BRAZIL ':'SOUTH AMERICA',
    'CHILE ':'SOUTH AMERICA',
    'BOLIVIA':'SOUTH AMERICA',
    'MADAGASCAR':'AFRICA',
    'GABON':'AFRICA',
    'SENEGAL ':'AFRICA',
    'GAMBIA':'AFRICA',
    'QATAR':'ME ASIA', 
    'BAHRAIN':'ME ASIA',
    'LEBANON ' :'ME ASIA',
    'BURKINA FASO':'AFRICA',
    'ALGERIA ':'AFRICA',
    'MALAWI':'AFRICA',
    'GUINEA':'AFRICA',
    'TOGO':'AFRICA',
    'LIBERIA':'AFRICA',
    'DJIBOUTI':'AFRICA',
    'VIETNAM ':'ASIA',
    'AUSTRALIA + NEW ZEALAND':'OCEANIA'
    }

origins_mapping={
    'ARGENTINA':'ARG',
    'URUGUAY':'UGY',
    'PARAGUAY':'PYG',
    
    'ARG':'ARG',
    'PY.':'PYG',
    'PY':'PYG',
    'PY ':'PYG',
    'URU':'UGY',
    'UY':'UGY',
    'BOL':'BOL',
    'BZ':'BRA',   }

port_mapping = {
    'SAN LORENZO': 'San Lorenzo',
    'TOYOTA': 'San Lorenzo',
    'San Lorenzo - Argentina':'San Lorenzo',
    'Cofco Int.':'San Lorenzo',
    'PORT':'San Lorenzo',

    
    'ROSARIO': 'Rosario',
    'SAN PEDRO': 'San Pedro',
    'San Pedro - Argentina':'San Pedro',
    
    'NUEVA PALMIRA(R.O.U.)':'Uruguay',
    'NUEVA PALMIRA':'Uruguay',
    'PALMIRA':'Uruguay',
    'Neuva Palmira':'Uruguay',
    'URUGUAY':'Uruguay',

    'BAHIA BLANCA': 'Bahia Blanca',
    'Bahia Blanca - Argentina':'Bahia Blanca',
    'Bahía Blanca - Argentina':'Bahia Blanca',
    'Bahía Blanca':'Bahia Blanca',
    

    'NECOCHEA': 'Necochea',
    'NECOCHEA / QUEQUEN':'Necochea',
    'necochea / quequen':'Necochea',
    'Necochea - Argentina':'Necochea',

    'BUENOS AIRES':'Buenos Aires',
    'SAN NICOLAS': 'San Nicolas',
    'ZARATE':'Zarate',
    'GUAZU': 'Zarate',
    'Guazú': 'Zarate',
    'Zárate':'Zarate',
    'PARANA GUAZU':'Zarate',
    'DEL GUAZU':'Zarate',
    'Paraná Guazú - Argentina':'Zarate',
    'DELTA':'Zarate',
    'LIMA':'Zarate',
    'Lima - Argentina':'Zarate',
    'LAS PALMAS':'Zarate',
    'Zárate - Argentina':'Zarate',
    'Parana Guazu - Argentina':'Zarate',
    'Zarate - Argentina':'Zarate',
    'IBICUY':'Zarate',

    'MONTEVIDEO': 'Uruguay',
    'Montevideo':'Uruguay',
    'MONTEVIDEO (R.O.U)':'Uruguay',
    'MONTEVIDEO(R.O.U)':'Uruguay',
    'FRAY BENTOS':'Uruguay',
    'PAYSANDU':'Uruguay',

    'CAMPANA': 'Campana',
    'Campana - Argentina': 'Campana',
    'campana': 'Campana',

    'GRAL LAGOS':'Rosario',
    'GRAL. LAGOS':'Rosario',
    'LAGOS':'Rosario',
    'Rosario - Argentina':'Rosario',
    'Villa Constitución - Argentina':'Rosario',
    'VILLA CONSTITUCION':'Rosario',
    'VCONSTITUCION':'Rosario',
    'V.CONSTITUCION':'Rosario',
    'Villa Constitucion - Argentina':'Rosario',
    'TIMBUES':'Rosario',
    'ARROYO SECO':'Rosario',
    
    'ESCOBAR':'Escobar',
    
    'SANTA FE':'Santa Fe',
    'CONCEP. DEL URUGUAY':'Concep. Del Uruguay',

    'DIAMANTE':'Diamante',

    'RAMALLO':'Ramallo',
    'RANALLO':'Ramallo',
    'Ramallo - Argentina':'Ramallo',

    'RIO GRANDE':'Rio Grande',
    
    'ALIANZA G2':'Alianza',
    'TRANSHIPMENT':'Transhipment',
    'SAN NICOLÁS':'San Nicolas',
    'San Nicolas - Argentina':'San Nicolas',
    'PORT':'PORT'
}
country_mapping={

    'DJBOUTI':'DJIBOUTI',
    'BRASIL':'BRAZIL',
    'BAHREIN':'BAHRAIN',
    'DOM. REP':'DOM REP',
    'DOM REP.':'DOM REP',
    'DOM. REP;':'DOM REP',
    'DOM. REP.':'DOM REP',
    'DOM. REP;':'DOM REP',
    'DOM.REPUBLIC':'DOM REP',
    'DOMINICAN REP':'DOM REP',
    'DOMINICAN REPUBLIC':'DOM REP',
    'CONGO DEM.REP.':'DOM REP',
    'DOM. REPUBLIC':'DOM REP',
    'DOMINICAN REP.':'DOM REP',
    'U.A.E.':'UAE',
    'EAU':'UAE',
    'U. ARAB EMIRATES':'UAE',
    'UNITED ARAB EMIRATES':'UAE',
    'U.ARAB EMIRAT':'UAE',
    'KUWAIT+UNITED ARAB EMIRATES':'UAE',
    'HOLANDA':'NETHERLANDS',
    'HOLLAND':'NETHERLANDS',
    'MAURITUIS':'MAURITIUS',
    'LEBANNON':'LEBANON',
    'US':'USA',
    'LITHUANIA':'LITUANIA',
    'Malaysia':'MALAYSIA',
    'IVORY COST':'IVORY COAST',
    'MALASYA':'MALAYSIA',
    'UNITED KINGDOM':'UK',
    'U.KINGDOM':'UK',
    'MAURITIUS ISL.':'MAURITIUS',
    'MAURITIUS IS':'MAURITIUS',
    'CHILE+PERU':'CHILE',
    'PTO RICO':'PUERTO RICO',
    'PTO. RICO':'PUERTO RICO',
    'COSTA RICA+GUATEMALA':'COSTA RICA',
    'CHLE':'CHILE',
    'EUROPEAN PORTS':'EU',
    'ALEGERIA':'ALGERIA',
    'NUEVA PALMIRA':'URUGUAY',
    'PTO RICO/DOM REP':'DOM REP',
    'KUWUAIT':'KUWAIT',
    'KWUEIT':'KUWAIT',
    'OMAN+KUWAIT+SAUDI ARABIA':'KUWAIT',
    'PERÚ':'PERU',
    'PERU/ECUADOR':'PERU',
    'DEMOCRATIC REPUBLIC OF THE CONGO':'CONGO',
    'REPUBLIC OF THE CONGO':'CONGO',
    'MATADI':'CONGO',
    'POINTE NOIRE':'CONGO',
    'DAKAR':'SENEGAL',
    'TAILANDIA':'THAILAND',
    'GREECE/ISRAEL':'GREECE',
    'GREECE/ITALY':'GREECE',
    'HOLLAND/GERMANY':'GERMANY',
    'IRAQ':'IRAK',
    'ISRAEL/GREECE':'ISRAEL',
    'MAURITANA':'MAURITANIA',
    'TRINIDAD & TOBAGO':'TRINIDAD',
    'PHILIPPINNES':'PHILIPPINES',
    'PHILIPINES':'PHILIPPINES',
    'MAURITUS IS':'MAURITIUS',
    'CYPRUS/GREECE':'GREECE',
    'NEW ZELAND':'NEW ZEALAND',
    'RUSSIAN FEDERATION':'RUSSIA'

    }   

month_mapping = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

shippers_mapping={

 'ACSA': 'DACSA',
 'ADECO AGROP.': 'ADECO AGROP',
 'ADECOAGRO': 'ADECO AGROP',
 'ADECO': 'ADECO AGROP',
 'AGRICORP': 'AGROCORP',
 'ALEA & CIA':'ALEA',
 'ADM ARG': 'ADM',
 'ADM AGRO': 'ADM',
 'ADM PAR': 'ADM',
 'ADM PY': 'ADM',
 'ADM, TBC': 'ADM',


 'AGP-GRAIN': 'AGP',
 'AGP GRAIN':'AGP',
 'AGRIBUSINES': 'AGRIBUSINESS',
 'AGRO BUSINESS': 'AGRIBUSINESS',
 'AGRO GAUCHO SA': 'AGRO GAUCHO',
 'AGRO INDUSTRIAS BAIRES': 'AGROINDUSTRIAS BAIRES',
 'AGROFINA S.A.': 'AGROFINA SA',
 'AGROINVERSIONES PAMPEANAS': 'AGRO INVERSIONES PAMPEANAS',
 'AGRONEGOCIOS JEWELL': 'AGRONEGOCIOS JEWEL SRL',
  'AGROEAN': 'AGROCEAN',
 'AGROOCEAN': 'AGROCEAN',
 'AGROPOR': 'AGROPORT',
 'ALG.AVELLANEDA': 'ALG. AVELLANEDA',
 'ALGHURAIR': 'AL GHURAIR',
 'ALIMENTOS & FORRA': 'ALIMENTOS',
 'ALIMIENTOS': 'ALIMENTOS',

 'ALMARAIS': 'ALMARAI',
 'ALNARAI': 'ALMARAI',
 'AMAGGI ': 'AMAGGI',
 'AMGGI': 'AMAGGI',
 'AMDERSONS': 'ANDERSONS',
 'APL CODRICO': 'APL-CODRICO',
 'BGH ': 'BGH',
 'BIO GRAIN': 'BIOGRAIN',
 'BIO-OILS': 'BIO OILS',
 'BOHAI': 'BOHI',
 'BRAINCORP': 'GRAINCORP',
 ' BUNGE': 'BUNGE',
 'BUINGE': 'BUNGE',
 'BUNE': 'BUNGE',
 'BUNGE BOLIV': 'BUNGE',
 'BUNGE GVA': 'BUNGE',
 'BUNGE PAR': 'BUNGE',
 'BUNGE PARAGUAY': 'BUNGE',
 'BUNGE PARAG': 'BUNGE',

 'C.CEREALES': 'CAM CEREALES',
 'CAN CEREALES': 'CAM CEREALES',
 'CAM ': 'CAM CEREALES',
 'CAFI': 'CAF',
 'CALVESTONE': 'CALVESTON',
 'CAMPOS VERDES ARGENTINOS': 'CAMPOS VERDES ARGENTINOS S.A.',
 
 'CARGIILL': 'CARGILL',
 'CARGIL': 'CARGILL',
 'CARGIL BOL': 'CARGILL',
 'CARGILL  ': 'CARGILL',
 'CARGILL UY': 'CARGILL',
 'CARGLL': 'CARGILL',
 'CARILL': 'CARGILL',
 'CARGILL AGROP':'CARGILL',
 'CARGILL AMERICAS INC.':'CARGILL',
 'CARGILL BOLIV':'CARGILL',
 'CARGILL INCORP.':'CARGILL',
 'CARGILL AGROP':'CARGILL',

 'CASILO': 'CASILLO',
 'CASSILO': 'CASILLO',
 'CASTILLO': 'CASILLO',

 'CC AGRO & TEC S.A.': 'C C AGRO & TEC S.A',
 'CEREAL DOCKS': 'CEREAL DOCK',
 'CEREALERA AZUL': 'CERALERA AZUL',
 'CEREALES MAGGIOLLO': 'CEREALES MAGGIOLO',
 'CEREALIS ': 'CEREALIS',
 'CEREOIL ': 'CEREOIL',
 'CERFLOY': 'CERFOLY',
 'CHINA AGRI OIL': 'CHINA AGRI OILS',
 'CHINA AGRIOILS': 'CHINA AGRI OILS',
 'CHS DE ARGENTINA': 'CHS',
 'CHS ARGENTINA': 'CHS',
 'CHS DE URUGUAY': 'CHS',

 'CÍA ARG DE GRANOS': 'CIA ARG DE GRANOS',
 'CLP CHEMICALS ': 'CLP CHEMICALS',

 'COCFCO': 'COFCO',
 'COFCO RESOURCES SA': 'COFCO',
 'COFCO AGRI ARG':'COFCO',
 'COFCO INT.':'COFCO',
 'COFCO INT. ARG.':'COFCO',
 'COFCO. ERRO':'COFCO',
 'COFCO INT.ARG':'COFCO',

 'COGENTRA': 'CONGENTRA',
 'CONTEGRAL ': 'CONTEGRAL',
 'COOP AVEL': 'COOP AVE',
 'COPAGRAN': 'COPAGRA',
 'CORP CEREALES': 'CORP. DE CEREALES',
 'CORP. CEREALES': 'CORP. DE CEREALES',
 'CORVEN MOTORS': 'CORVEN MOTOR',
 'CROSLAND': 'CROSSLAND',
 'CURICJA': 'CURCIJA',
 'CURSIJA': 'CURCIJA',
 'DAEWOO': 'DAEWO',
 'DERIVADOS VÍNICOS SA': 'DERIVADOS VINICOS',
 'DESA': 'DESAB',
 'DIAZ & FORTI SRL': 'DIAZ & FORTI',
 'DIAZ FORTI': 'DIAZ & FORTI',
 'DIAZ Y FORTI': 'DIAZ & FORTI',
 'DIMITRIAKI ': 'DIMITRIAKI',
 'E-GRAIN': 'E GRAIN',
 'EGRAIN': 'E GRAIN',
 'E.CRUSHING': 'ER CRUSHING',
 'E.R. CRUSHING': 'ER CRUSHING',
 'ERCRUSHING': 'ER CRUSHING',
 'ED & FMAN': 'ED & F MAN',
 'ENGLEHART': 'ENGELHART',
 'ERCA': 'ERCSA',
 'EST.SAN PATRICIO': 'SAN PATRICIO',
 'ESTAB.AGROP.DEL SUDESTE SA': 'EST.AGROP DEL SUDESTE',
 'ETA-DUBAI': 'ETA DUBAI',
 'FACCIUTO FERNANEZ': 'FACCIUTO FERNANDEZ',
 'FERERAL AGROPECUARIA': 'FEDERAL AGROPECUARIA',
 'FONDEMONTE S.A.': 'FONDOMONTE',
 'FONDO MONTE': 'FONDOMONTE',
 'FONDOMONTE': 'FODOMONTE',
 'FONDOMONTE SOUTH AMERICA SA': 'FONDOMONTE',
  'FONDOMONTE SOUTH AMERICA': 'FONDOMONTE',

 'GANADERIA INTEGRAL NICARAGUA S.A.': 'GANADERIA INTEGRAL NICARAGUA',
 'GAVILLON': 'GAVILON',
 'GLCENCORE': 'GLENCORE',
 'GLENCONRE': 'GLENCORE',
 'GLENCOR': 'GLENCORE',
 'GM COMMODITIES': 'COMMODITIES',
 'GRAIN CORP': 'GRAINCORP',
 'GRANELES ': 'GRANELES',
 'GRANELES DE CHILE': 'GRANELES',
 'GRANES': 'GRANELES',
 'GRANELES ANDINOS': 'GRANELES',
 'GREEN ENERGY': 'GREENERGY',
 'GROBOCOPATEL HERMANOS': 'GROBOCOPATEL  HNOS',
 'GRVETAL': 'GRAVETAL',
 'IMP.Y EXP DEL NORTE': 'IMP.& EXP DEL NORTE',
 'IN VIVO': 'INVIVO',
 'IND DE ACEITE': 'IND ACEITE',
 'IND. DE ACEITE SA': 'IND DE ACEITE',
 'INDAGRO': 'INAGRO',
 'INGREDION SA': 'INGREDION ARG',
 'INTEPEC': 'INTERPEC',
 'INTERGRAIN': 'INTERGRAIN SA',
 'IOL SA': 'IOL',
 'IOLSA': 'IOL',
 
 'ITACOL': 'ITALCOL',
 'JC INTERNATIONAL': 'CJ INTERNATIONAL',
 'JTB GLOBAL SERVICES': 'JIT GLOBAL SERVICES',
 'LA TRANQUERA VERDE': 'LA TRANQUERA VERDE SA',
 'LDC ': 'LDC',
 'LDC PAR': 'LDC',
 'LDC PY': 'LDC',
 'LDC PARAGUAY': 'LDC',
 'DREYFUS':'LDC',
 'DREYFUS PAR':'LDC',
 'DREYFUS PARAGUAY':'LDC',
 
 'LEVESTOCK': 'LIVESTOCK',
 'LIFESTOCK': 'LIVESTOCK',
 'LIVESTOCK FD': 'LIVESTOCK',
 'LIVESTOCK FEED': 'LIVESTOCK',
 'LOSUR OVERSEAS': 'LOSUR OVERSEAS SL',
 'LUBRIFICANTI': 'LUBIRIFICANTI',
 'LUIS DUCRET': 'LUIS A DUCRET',
 'MALT. PAMPA': 'MALT PAMPA',
 'MALT PAMPA ': 'MALT PAMPA',
 'MANAGRO': 'MAN AGRO',
 'MARINE OLIE': 'MARINE OIL',
 'MALT.QUILMES':'MALT. QUILMES',
 'MED SOFTS': 'MEDSOFTS',
 'MEDSOFT': 'MEDSOFTS',
 'MEERA': 'MERA',
 'MERA INT': 'MERA',

 'MIDLE GRAIN': 'MIDDLE GRAIN',
 'MILCORP': 'MILICORP',
 'MILICORP TRADING': 'MILICORP',
 'MILLCORP': 'MILCORP',
 'MILLCORP TRADING': 'MILICORP',
 'MOHINO PAULISTA': 'MOLINO PAULISTA',
 'MOIHNO PAULISTA': 'MOLINO PAULISTA',
 'MOLINO PAULISTA': 'MOLINO PAULISTA',
 'MOLINO CAÑUEALAS': 'MOLINO CANUELAS',
 'MOLINO CAÑUELAS': 'MOLINO CANUELAS',
 'MOLINOS CAÑUELAS': 'MOLINO CANUELAS',
 'MOLINO PACIFICO': 'MOLINOS PACIFICO',
 'MOLINOS ': 'MOLINOS',
 'MOLINOS AGRO': 'MOLINOS AGRO S.A',
 'MOLINOS AMERICANOS': 'MOLINO AMERICANO',
 'MORENO': 'O.MORENO',

 'NIDERA ': 'NIDERA',
 'NIDERA UY': 'NIDERA',
 'NIIDERA': 'NIDERA',
 'NOBE': 'NOBLE',
 'NOBEGRAIN': 'NOBLE',
 'NOT AVAILABLE': '(NOT AVAILABLE)',
 'NUTIOIL': 'NUTRIOIL',
 'NUTRI OIL': 'NUTRIOIL',
 'NUTRIIOL': 'NUTRIOIL',
 'NUTRIOL': 'NUTRIOIL',
 'NUTROIL': 'NUTRIOIL',

 'O MORENO': 'MORENO',
 'P CREMER': 'CREMER',
 'P. CREMER': 'P CREMER',
 'PAN AMERICAN': 'PANAMERICA',
 'PAN AMERICAN GRAINS': 'PANAMERICA',
 'PANAMERICAN': 'PANAMERICA',
 'PANAMERICANA': 'PANAMERICA',
 'PARAMERICA': 'PANAMERICA',

 'PERDUE AGRIBISINESS': 'PERDUE AGRIBUSINESS',
 'PERDUE AGRIBUSINESS': 'PERDUE AGRIBUSINESS',
 'PETROAGRO SA': 'PETROAGRO',
 'PRO AGRO SA': 'PETROAGRO',
 'PROAGRO': 'PETROAGRO',

 'PHEATON': 'PHAETON',
 'PRADERA NATURAL': 'PRADERA NATURAL SA',
 'PRIMINDS': 'PREMINDS',

 'PTO ARROYO SECO': 'PTO ARROYO SECO SRL',
 'RAUL H PEREZ': 'RAUL H PEREZ',
 'RAUL H.PEREZ': 'RAUL H PEREZ',
 'RECOUP ': 'RECOUP',
 'SALTO AGUARA': 'SALTO AGUARAY',
 'SAN FERNADO': 'SAN FERNANDO',
 'SAN FERNANDA': 'SAN FERNANDO',
 'SCORCIELLO': 'SCORZIELLO',
 'SEABOARD ': 'SEABOARD',
 'SEABORAD': 'SEABOARD',
 'SEABORD': 'SEABOARD',
 'SIRENTZ': 'SIERENTZ',
 'SODRUDESTVO': 'SODRUGESTVO',
 'SODRUGESTIVO': 'SODRUGESTVO',
 'SODRUGETSVO': 'SODRUGESTVO',
 'SODRUGETZVO': 'SODRUGESTVO',
 'SODRUJESTVO': 'SODRUGESTVO',

 'SORPODI': 'SOPRODI',
 'SOYA MIILLS': 'SOYA MILLS',
 'SOYA MILLS ': 'SOYA MILLS',
 'SOYAMILLS': 'SOYA MILLS',
 'SPECIAL GRAIN': 'SPECIAL GRAINS',
 'SPECIAL GRAINS': 'SPECIAL GRAINS',
 'SPECIAL G.': 'SPECIAL GRAINS',
 'SPECIAL GRAINS SA': 'SPECIAL GRAINS',
 
 'TIRIYAKI': 'TIRYAKI',
 'TIRYAKY': 'TIRYAKI',
 'TIRYALI': 'TIRYAKI',
 'TRIYAKI': 'TIRYAKI',
 'TOEPFR': 'TOEPFER',
 'TOPEFER': 'TOEPFER',
 'TORNULAR': 'TORUNLAR',
 'TOYOTA  TSUSHO': 'TOYOTA TSUSHO',
 'UNION AGRIC AVELLANEDA': 'UNION AGRÍCOLA AVELLANEDA ',
 'UNION AGRÍCOLA AVELLANEDA ':'UNION AGRICOLA AVELLANEDA',

 'URCCOPA': 'URCOOPA',
 'URCOOPA PROVAL': 'URCOOPA',
 'URCOPA': 'URCOOPA',
 'VARIOS': 'VARIOUS',
 'VICENTÍN': 'VICENTIN',
 'VILUCO ': 'VILUCO',
 'VITERRA ACOPIO S.A.':'VITERRA',
 'VITERRA ACOPIO S.A':'VITERRA',
 'VITERRA ARGENTINA S.A':'VITERRA',
 'VITERRA ARGENTINA SA':'VITERRA',
 'VITERRA ARGENTINA SA+YPF':'VITERRA',

 'WELLINGTON': 'WILLMINGTON',
 'WILGMINTON': 'WILLMINGTON',
 'YELLOW ROCK': 'YELLOWROCK',
 'YELOWROCK': 'YELLOWROCKK',
 'ZEN-NOH': 'ZEN NOH',
 'ZENNOH': 'ZEN NOH'}

bahia_berth_mapping = {
    'tbb 9':'TBB', # Port is always Bahia (1 san lorenzo)
    'bb 9':'TBB',
    'tbb':'TBB',# Port is always Bahia (1 san lorenzo)
    'tbb9':'TBB',# Port is always Bahia (1 san lorenzo)
    'terminal bahia blanca berths 9':"TBB", 
    'tbb 5/6':'TBB',
    'bb 26':'TBB',
    '5/6':'TBB',
    'adm agro bahía blanca (ex toepfer)':'TBB',
    'adm agro bahía blanca':'TBB',
    'adm agro bahia blanca':'TBB',
    '5 pg':'TBB',
    'pier 5/6':'TBB',
    
    'dreyfus terminal':'LDC Bahia Blanca', #Every port is Bahia
    'dreyfus grain terminal':'LDC Bahia Blanca',
    'ldc bahia blanca':'LDC Bahia Blanca',
    'LDC Bahia Blanca':'LDC Bahia Blanca',
    
    'terminal bahia blanca':'TBB',

    'cargill terminal':'Puerto Ing White', #Port is Bahia
    'cargill (bb)':'Puerto Ing White', 
    'puerto ing white':'Puerto Ing White',
    'cargill':'Puerto Ing White',
    'Puerto Ing White':'Puerto Ing White',
    
    'galvan 2/3':'Puerto Galvan',
    'galvan':'Puerto Galvan',
    'glencore':'Puerto Galvan', #bahia
    'gt/ute':'Puerto Galvan',
    'ute':'Puerto Galvan', #bahia
    'pier 9':'Puerto Galvan', #Bahia
    'omhsa terminal': 'Puerto Galvan', #Bahia
    'viterra puerto galvan':'Puerto Galvan',
    'berths 2 / 3 viterra (ex moreno) puerto galván':'Puerto Galvan',
    '2/3 pg': 'Puerto Galvan',
    'pg 2/3':'Puerto Galvan',
    'puerto galvan':'Puerto Galvan',
    'ohmsa':'Puerto Galvan',

    'adm agro':'ADM Bahia Blanca',
    'el transito':'ADM Bahia Blanca',
    'adm agro (ex toepfer)':'ADM Bahia Blanca',
    'adm agro (ex l.piedra/toepfer)':'ADM Bahia Blanca',
    'adm':'ADM Bahia Blanca',
    'toepfer':'ADM Bahia Blanca',
}
 
rosario_berth_mapping={
    'punta alvear': 'Punta Alvear',
    'punta': 'Punta Alvear',
    'p.alvear':'Punta Alvear',
    'cargill punta alvear':'Punta Alvear',
    'Punta Alvear':'Punta Alvear',
    
    'serviport 1':"TPR", #Port is San Pedro --> Elevator?
    'serviport 6':"TPR", #Port is Rosario
    'serviport 7':"TPR", #Port is Rosario
    'serviport 2':'TPR', #Port is Villa constitucion
    'serviport 3':'TPR', # 1 row, rosario
    'servicios portuarios':'TPR', #Villa constitucion
    'serv.port.':'TPR',#Villa constitucion
    'serv port':'TPR',#Villa constitucion
    'servicios portuarios (unit 2)':'TPR',
    'villa constitucion':'TPR',
    'terminal puerto rosario (open berth) new mole south':'TPR',
    'u6':'TPR', #Rosario
    'u7':'TPR', #Rosario
    'unit 6':'TPR',
    'unidad 6':'TPR', # Rosario
    'unidad 7':'TPR',# Rosario
    'unit vii':'TPR', #port is Rosario
    'unit vi':'TPR', #port is Rosario
    'unit vi rosario':'TPR',
    'unit vii rosario':'TPR',
    'unidad 9':'TPR', 
    'unidad 11':'TPR', 
    'unidad 10':'TPR',
    'unidad 12':'TPR',
    'unidad 8':'TPR',
    'u.6':'TPR',
    'unit 6':'TPR',
    'unit 7':'TPR',
    'terminal puerto':'TPR',
    'm.nvo-s': 'TPR',
    'm.nvo-n': 'TPR',
    "new mole south (terminal puerto rosario)": "TPR",
    "NEW MOLE SOUTH (TERMINAL PUERTO ROSARIO)": "TPR",
    "tpr":"TPR",
    'new port':'TPR',
    'TPR':'TPR',
    'arroyo seco':'Arroyo Seco',
    'arroyo':'Arroyo Seco',
    'term.a.seco':'Arroyo Seco',
    'adm agro - arroyo seco':'Arroyo Seco',
    'term. a. seco':'Arroyo Seco',
    'arroyo seco terminal':'Arroyo Seco',
    'toepfer arroyo seco':'Arroyo Seco',
    'adm agro arroyo seco (ex a.seco terminal)':'Arroyo Seco',
    'adm agro arroyo seco':'Arroyo Seco',
    'adm agro arroyo seco (ex toepfer)':'Arroyo Seco',
    'adm agro (ex toepfer)':'Arroyo Seco',
    'toepfer':'Arroyo Seco',
    'adm':'Arroyo Seco',
    'adm agro san lorenzo':'Arroyo Seco',

    
    'lagos':'LDC Gral Lagos',
    'dreyfus terminal gral. lagos':'LDC Gral Lagos',
    'gl':'LDC Gral Lagos',
    'dreyfus dry cargo terminal':'LDC Gral Lagos', 
    'ldc gral lagos': 'LDC Gral Lagos',
    'dreyfus vegoil terminal': 'LDC Gral Lagos',
    'gral.lagos': 'LDC Gral Lagos',
    'LDC Gral Lagos': 'LDC Gral Lagos',
    'dreyfus vgoil berth gral. lagos':'LDC Gral Lagos',
    'dreyfus liquids terminal':'LDC Gral Lagos',

    'vgg':'VGG',
    'galvez':'VGG',
    'vgg term': 'VGG',
    "villa gobernador galvez": "VGG",
    'v.g.g':'VGG',
    'v.g.g.':'VGG',
    'crane':'VGG',
    'floating crane':'VGG',
    'cargill':'VGG',
    'VGG':'VGG',
    'cargill vgg':'VGG'
}

san_lorenzo_berth_mapping={
    "noble": "Cofco Timbues",
    'timbues noble':"Cofco Timbues",
    'cofco':'Cofco Timbues',
    "noble timbues": "Cofco Timbues",
    "noble floating berth": "Cofco Timbues",
    "cofco agri floating berth": "Cofco Timbues",
    'cofco agri timbues': 'Cofco Timbues',
    'cofco int. timbues':'Cofco Timbues',
    'cofco timbues':'Cofco Timbues',
    'noble timb':'Cofco Timbues',
    'cofco agri':'Cofco Timbues',
    'cofco agri lima (site a)':'Cofco Timbues',
    'cofco int. timbúes':'Cofco Timbues',
    'cofco int. lima (site a)':'Cofco Timbues',
    'Cofco timbues':'Cofco Timbues',
    'Cofco PGSM':'Cofco Timbues',
    'Cofco Timbues':'Cofco Timbues',
    'cofco int. timbues fl.berth':'Cofco Timbues',

    'transito': 'El Transito',
    'el transito (adm agro)':'El Transito',
    'adm': 'El Transito',
    "transito (adm agro)": "El Transito",
    'adm agro san lorenzo (ex tránsito)': "El Transito",
    'adm agro':'El Transito',
    'el transito':'El Transito',
    'adm agro (ex toepfer)':'El Transito',
    'adm agro (ex l.piedra/toepfer)':'El Transito',
    'l.piedrabuena/toepfer termin':'El Transito',
    'transito (adm agro)':'El Transito',
    'adm agro arroyo seco (ex toepfer)':'El Transito',
    'adm agro san lorenzo':'El Transito', 
    'molca':'El Transito',
    'tránsito':'El Transito',
    "quebracho": "Quebracho",
    "qubracho": "Quebracho",
    

    'dempa': 'Bunge PGSM',
    'pampa': 'Bunge PGSM',
    'pampra': 'Bunge PGSM',
    
    'bunge grain':'Bunge PGSM',
    "bunge terminal": "Bunge PGSM",
    "bunge": "Bunge PGSM",
    'buenge terminal':'Bunge PGSM',
    "bunge fert": "Bunge PGSM",
    'bunge pgsm':'Bunge PGSM',

    "nidera": "Cofco PGSM",
    'cofco pgsm': 'Cofco PGSM',
    'cofco intl. south berth ex noble)':'Cofco PGSM',
    'cofco intl pgsm south berth          (ex nidera )':'Cofco PGSM',
    'cofco intl. south berth (ex noble)':'Cofco PGSM',
    'cofco argentina south berth':'Cofco PGSM',
    'cofco intl. south berth':'Cofco PGSM',
    'cofco intl. pgsm south berth (ex nidera)':'Cofco PGSM', 
    'cofco int. pgsm south':'Cofco PGSM',
    "nidera fertilizers": "Cofco PGSM",
    'nidera fert.': 'Cofco PGSM',
    'nidrea':'Cofco PGSM',
    'cofco pgsm south (ex nidera)':'Cofco PGSM',
    'cofco pgsm south':'Cofco PGSM',
    'cofco intl. north berth':'Cofco PGSM',
    'cofco intl. pgsm north berth (ex nidera fertilizantes)':'Cofco PGSM',
    'cofco int. pgsm north':'Cofco PGSM',


    'terminal 6 norte':'T6 - Resinfor',
    'terminal 6 sur':'T6 - Resinfor',
    'terminal 6':'T6 - Resinfor',
    't6':'T6 - Resinfor',
    'arauco argentina':"T6 - Resinfor",
    "term.6-n": "T6 - Resinfor",
    "term.6-s": "T6 - Resinfor",
    'berth 6': 'T6 - Resinfor',
    'resinfor': 'T6 - Resinfor',
    'terminal 6-north':'T6 - Resinfor',
    't6 - resinfor':'T6-Resinfor',
    'terminal 6  sur':'T6-Resinfor',
    'terminal 6 (n) north (s) south berth - tbc':'T6-Resinfor',
    'terminal 6-pier tbc':'T6-Resinfor',
    'terminal 6 (s) south berth':'T6-Resinfor',
    'terminal 6 (n) north berth':'T6-Resinfor',
    'terminal 6-north berth':'T6-Resinfor',
    'terminal 6-south berth':'T6-Resinfor',
    'terminal vi':'T6-Resinfor',
    'terminal 6-south':'T6-Resinfor',
    'arauco':'T6-Resinfor',
    
    'a.c.a.':"ACA San Lorenzo",
    'aca':"ACA San Lorenzo",
    'a.c.a':"ACA San Lorenzo",
    'a.c.a. timbues':'ACA San Lorenzo',
    'aca san lorenzo':'ACA San Lorenzo',
    'aca timbues':'ACA San Lorenzo',
    'ACA San Lorenzo':'ACA San Lorenzo',
    'vicentin': 'Vicentin Port',
    'vincentin': 'Vicentin Port',
    'vicentin port': 'Vicentin Port',

    'san benito': 'San Benito',
    'san benigto':'San Benito',
    'sam benito': 'San Benito',
    'vietnam':'San Benito',
    '<':'San Benito',

    'ldc timbúes': 'LDC Timbues',
    "dreyfus timbues": "LDC Timbues",
    'timbues ldc':"LDC Timbues",
    'ldc timbues': 'LDC Timbues',
    'dreyfus vegoil terminal':'LDC Timbues',
    'ldc':'LDC Timbues',
    'lcd':'LDC Timbues',
    'LDC Timbues':'LDC Timbues',
    'LDC timbues':'LDC Timbues',
    'dreyfus terminal':'LDC Timbues',
    'ldc timbúes': 'LDC Timbues',

    'renova (north berth)':'Renova',
    'renova north berth':'Renova',
    'renova south':'Renova',
    'renova north':'Renova',
    'renova term': 'Renova',
    "renova south berth": "Renova",
    'renova':'Renova',
    'renova-north':'Renova',
    'renova-south':'Renova',
    'a.g.d. timbues (aceitera general deheza)':'AGD Timbues',#1 row San lorenzo
    'a.g.d. timbues':'AGD Timbues',
    'agd timbues':'AGD Timbues',
    'AGD Timbues':'AGD Timbues',
    'terminal g':'AGD Timbues',
    'akzo nobel': 'Nouryon - Akzo Nobel',
    'nouryon /ex akzo nobel': 'Nouryon - Akzo Nobel',
    'nouryon - akzo nobel':'Nouryon - Akzo Nobel',
    'nouryon chemicals argentina':'Nouryon - Akzo Nobel',
    
    'minera alumbrera':'T6 - Minera Alumbrera',
    't6 - minera alumbrera':'T6 - Minera Alumbrera'
}

uruguay_berth_mapping={
    'montevideo': 'Montevideo',
    'pier c': 'Montevideo',
    'pier b': 'Montevideo',
    'transgranel':'Montevideo', #Montevideo
    'obrinel':'Montevideo',#Montevideo
    'terminal granelera obrinel':'Montevideo',#Montevideo
    'tgo':'Montevideo', #Montevideo
    'tgm terminal':'Montevideo',
    'tgm':'Montevideo',
    'pier 6-7':'Montevideo',
    'navios':'Nueva Palmira',
    'navios terminal': 'Nueva Palmira', 
    'navios terminal south': 'Nueva Palmira', 
    'navios terminal north': 'Nueva Palmira',
    'terminal navios north': 'Nueva Palmira', 
    'navios pier 1': 'Nueva Palmira',
    'navios pier 3':'Nueva Palmira',
    'navios pier 4':'Nueva Palmira',
    'terminal navios south':'Nueva Palmira',
    't.g.u.':'Nueva Palmira', #port always NP
    'tgu':'Nueva Palmira',
    'anp north pier':'Nueva Palmira',
    'tgu/anp north section':'Nueva Palmira',
    'tgu terminal/anp north':'Nueva Palmira',
    'tgu (anp nort section)':'Nueva Palmira',
    'tgu/anp/north section':'Nueva Palmira',
    'anp':'Nueva Palmira',
    'a.g.d. timbues':'Nueva Palmira',
    'tgu pier north':'Nueva Palmira',
    'tgu (anp north section)':'Nueva Palmira',
    'tgu/anp north':'Nueva Palmira',
    'ontur':'Nueva Palmira', #Port is nueva palmira
    'ontur terminal':'Nueva Palmira',
    'antwerpen':'Nueva Palmira', #nueva palmira
    'anw':'Nueva Palmira',#nueva palmira
    'anp/north section':'Nueva Palmira',#nueva palmira
    'fiorucci':'Nueva Palmira',#nueva palmira
    'fiorucci international':'Nueva Palmira',
    'sitio 0':'Nueva Palmira',#nueva palmira
    'transf':'Nueva Palmira', #nueva palmira
    'tgu north pier':'Nueva Palmira',
    'nueva palmira':'Nueva Palmira',
    'paysandu':'Paysandu',
    'fray bentos':'Fray Bentos',
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

def extract_date_or_default(row, default_days=0):
    # Check if the row contains "Alongside"
    if 'Alongside' in row:
        return datetime.today()  # Return today's date if "Alongside" is found
    
    # Regular expression to find dates in dd/mm/yyyy format
    match = re.search(r'\d{2}/\d{2}/\d{4}', row)
    if match:
        return pd.to_datetime(match.group(), format='%d/%m/%Y')
    else:
        return datetime.today() + timedelta(days=default_days)
    
def extract_date(row):
    for col in ['ETA', 'ETB', 'ETF']:
        if pd.notna(row[col]):
            date_str = row[col][-5:]
            try:
                return pd.to_datetime(date_str + '/2024', format='%d/%m/%Y')
            except ValueError:
                continue
    return np.nan

def rebuild_date(value):
    value_str = str(value)  # Ensure it's a string
    if value_str.startswith('2024'):
        # Extract parts to form 'dd/mm/yy'
        return f"{value_str[-2:]}/{value_str[5:7]}/{value_str[2:4]}"
    return value_str  # Leave other values as is

def process_dates(value):
    value_str = str(value)  # Ensure the value is a string
    if value_str.startswith('2024'):
        return value_str[:10]  # Take the first 10 characters
    else:
        return value_str[:8]  # Take the first 8 characters

def map_berth(row):
    port = row['Port']
    berth = row['Berth']
    flag = ''  # Default flag value

    # Determine the mapping based on port
    if port == 'Bahia Blanca':
        mapped_berth = bahia_berth_mapping.get(berth, None)
    elif port == 'Rosario':
        mapped_berth = rosario_berth_mapping.get(berth, None)
    elif port == 'San Lorenzo':
        mapped_berth = san_lorenzo_berth_mapping.get(berth, None)
    elif port == 'Uruguay':
        mapped_berth = uruguay_berth_mapping.get(berth, None)
    elif port == 'Necochea':
        mapped_berth = 'Necochea'
    elif port == 'Nueva Palmira':
        mapped_berth = 'Nueva Palmira'
    elif port == 'Campana':
        mapped_berth = 'Carboclor'
    elif port == 'Diamante':
        mapped_berth = 'Diamante'
    elif port == 'Parana':
        mapped_berth = 'Parana'
    elif port == 'Ramallo':
        mapped_berth = 'Ramallo'
    elif port == 'Rio Grande':
        mapped_berth = 'Rio Grande'
    elif port == 'San Nicolas':
        mapped_berth = 'Elevador San Nicolas'
    elif port == 'San Pedro':
        mapped_berth = 'Elevador San Pedro'
    elif port == 'Zarate':
        mapped_berth = 'Del Guazu'
    elif port == 'Buenos Aires':
        mapped_berth = 'Terbasa 1'
    elif port == 'Alianza':
        mapped_berth = 'Alianza G2'
    elif port == 'Escobar':
        mapped_berth = 'Top off'
    elif port == 'Santa Fe':
        mapped_berth = 'Elevador'
    elif port == 'CONTAINERS':
        mapped_berth = 'Containers'
    elif port == 'Concep. Del Uruguay':
        mapped_berth = 'Concep. Del Uruguay'   
    else:
        mapped_berth = None  # Unhandled port

    # If no mapping is found, update the flag
    if mapped_berth is None:
        flag = 'To be checked'
        mapped_berth = berth  # Retain original berth

    return pd.Series([mapped_berth, flag])



# COMMAND ----------

# MAGIC %md
# MAGIC # Merger History and New Lineups

# COMMAND ----------

history=sp_mgr.read_pd_from_excel('/sites/GRP-TradingLineups/Lineups/merged/history and lineups.xlsx')

history_w=history[history['Product']=='Wheat']
history_corn=history[history['Product']=='Corn']
history_barley=history[history['Product']=='Barley']
history_sbo=history[history['Product']=='SBO']

sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/GRAINS/Corn/Corn lineup lw.xlsx',history_corn,index=False)
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/GRAINS/Barley/Barley lineup lw.xlsx',history_barley,index=False)
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/GRAINS/Wheat/Wheat lineup lw.xlsx',history_w,index=False)
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/OILS/sbo lineup lw.xlsx',history_sbo,index=False)

lineups_merged=sp_mgr.read_pd_from_excel('/sites/GRP-TradingLineups/Lineups/Merged/Lineups_merged.xlsx')

history['Region'] = history['Region'].map(lambda x: country_region_mapping.get(x, x))

history['Shipper'] = history['Shipper'].str.upper()
history['Coordinator'] = history['Coordinator'].str.upper()

history['Shipper'] = history['Shipper'].map(lambda x: shippers_mapping.get(x, x))
history['Coordinator'] = history['Coordinator'].map(lambda x: shippers_mapping.get(x, x))

history['Coordinator'] = history['Coordinator'].apply(lambda x: x if isinstance(x, str) else 'UNKNOWN')
history['Shipper'] = history['Shipper'].apply(lambda x: x if isinstance(x, str) else 'UNKNOWN')

history.loc[(history['Quantity'] == 'tbc'), ['Quantity']] = np.nan

history = history.drop_duplicates()

history['Date'] = pd.to_datetime(history['Date'])

lineups_merged['Date'] = pd.to_datetime(lineups_merged['Date'])

# Get the current date
now = datetime.now()

# Check if the current day is less than 7
if now.day < 7:
    # Move to the first day of the previous month
    current_month_start = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
else:
    # Otherwise, use the current month's start
    current_month_start = now.replace(day=1)

history = history[~((history['Date'] < current_month_start) & (history['Status'] == 'ANNOUNCED'))]

# Filter out rows in history with dates from the current month onward
# Keep rows before current_month_start OR with Flag == 'x'
history = history[(history['Date'] < current_month_start) | (history['Flag'].str.lower() == 'x')]

# Filter rows in lineups_merged with dates from the current month onward
lineups_current_and_after = lineups_merged[lineups_merged['Date'] >= current_month_start]

# Identify vessels in history marked with Flag == 'x'
vessels_with_flag_x = history.loc[history['Flag'].str.lower() == 'x', 'Vessel']

# Identify vessels in lineups_current_and_after
vessels_in_lineups = lineups_current_and_after['Vessel'].unique()

# Filter out rows in history where Flag == 'x' and Vessel is also in lineups_current_and_after
history = history[~((history['Vessel'].isin(vessels_in_lineups)) & (history['Flag'].str.lower() == 'x'))]


# Append the filtered rows from lineups_merged to history
updated_history = pd.concat([history, lineups_current_and_after])

updated_history = updated_history.drop_duplicates()
updated_history.loc[(updated_history['Berth']=='UNKNOWN') & (updated_history['Flag'] == 'To be checked'), 'Flag'] = ''
updated_history.loc[(updated_history['Process']=='Maize'), 'Process'] = 'Corn'

sp_mgr.rm('/sites/GRP-TradingLineups/Lineups/merged/history and lineups.xlsx')
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/merged/history and lineups.xlsx',updated_history,index=False)


history_w=updated_history[updated_history['Product']=='Wheat']
history_corn=updated_history[updated_history['Product']=='Corn']
history_barley=updated_history[updated_history['Product']=='Barley']
history_sbo=updated_history[updated_history['Product']=='SBO']

sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/GRAINS/Corn/Corn lineup actual.xlsx',history_corn,index=False)
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/GRAINS/Barley/Barley lineup actual.xlsx',history_barley,index=False)
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/GRAINS/Wheat/Wheat lineup actual.xlsx',history_w,index=False)
sp_mgr.save_pd_to_excel('/sites/GRP-TradingLineups/Lineups/PROVIDERS/OILS/sbo lineup actual.xlsx',history_sbo,index=False)


# COMMAND ----------


