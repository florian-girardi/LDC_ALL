# Databricks notebook source
# MAGIC %pip install LDCCropMonitor LDCGeolocation LDCGEETools earthengine-api

# COMMAND ----------


from LDCDataAccessLayerPy import databricks_init, SqlManager
databricks_init('GO')
import LDCCropMonitor, LDCGeolocation, LDCGEETools
import pandas as pd
import random
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
sql = SqlManager()



# COMMAND ----------

DB = 'Datacuration-RemoteSensing'
# You can add multiple fc_extensions to the list below
fc_extensions = ['BR_Corn_Summer']
# Don't need to change anything below this
indicators_views = {
  'SoilMoistureI': [
    {'name': 'SMI', 'view': 'EcmwfEra5landSoilmoistureIDailyView', 'column': 'SoilMoistureI'},
    {'name': 'SMISTFC', 'view': 'EcmwfEra5landSoilmoistureIDailyForecastView', 'column': 'SoilMoistureI'}
    ],
  'SoilMoistureII': [
    {'name': 'SMII', 'view': 'EcmwfEra5landSoilmoistureIIDailyView', 'column': 'SoilMoistureII'},
    {'name': 'SMIISTFC', 'view': 'EcmwfEra5landSoilmoistureIIDailyForecastView', 'column': 'SoilMoistureII'}
    ],
  # 'SoilMoistureIII': [
  #   {'name': 'SMIII', 'view': 'EcmwfEra5landSoilmoistureIIIDailyView', 'column': 'SoilMoistureIII'},
  #   {'name': 'SMIIISTFC', 'view': 'EcmwfEra5landSoilmoistureIIIDailyForecastView', 'column': 'SoilMoistureIII'}
  #   ],
  'MinTemperature': [
    {'name': 'Tmin', 'view': 'EcmwfEra5landTemperatureDailyView', 'column': 'MinTemperature'},
    {'name': 'TminSTFC', 'view': 'EcmwfEra5landTemperatureDailyForecastView', 'column': 'MinTemperature', 'source': 'stfc'},
    {'name': 'TminLTFC', 'view': 'EcmwfEra5landTemperatureDailyForecastView', 'column': 'MinTemperature', 'source': 'ltfc'},
  ],
  'Temperature': [
    {'name': 'Tavg', 'view': 'EcmwfEra5landTemperatureDailyView', 'column': 'AvgTemperature'},
    {'name': 'TavgSTFC', 'view': 'EcmwfEra5landTemperatureDailyForecastView', 'column': 'Temperature', 'source': 'stfc'},
    {'name': 'TavgLTFC', 'view': 'EcmwfEra5landTemperatureDailyForecastView', 'column': 'Temperature', 'source': 'ltfc'},
  ],
  'MaxTemperature': [
    {'name': 'Tmax', 'view': 'EcmwfEra5landTemperatureDailyView', 'column': 'MaxTemperature'},
    {'name': 'TmaxSTFC', 'view': 'EcmwfEra5landTemperatureDailyForecastView', 'column': 'MaxTemperature', 'source': 'stfc'},
    {'name': 'TmaxLTFC', 'view': 'EcmwfEra5landTemperatureDailyForecastView', 'column': 'MaxTemperature', 'source': 'ltfc'},
  ],
  'Precipitation': [
    {'name': 'Rain', 'view': 'JaxaGsmapRainfallDailyView', 'column': 'Precipitation'},
    {'name': 'RainSTFC', 'view': 'JaxaGsmapRainfallDailyForecastView', 'column': 'Precipitation', 'source': 'stfc'},
    {'name': 'RainLTFC', 'view': 'JaxaGsmapRainfallDailyForecastView', 'column': 'Precipitation', 'source': 'ltfc'},
  ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Amount of AdmIds with data for the last 30 days

# COMMAND ----------

df = pd.DataFrame()
for fc in fc_extensions:
  for indic_name, view_specs in indicators_views.items():
    for view_spec in view_specs:
      view = view_spec['view']
      indic_abr = view_spec['name']
      res = sql.sql_query(DB, f"Select fc_extension, '{indic_abr}' as source_abbrev, date, count(distinct AdmId) as count_adm FROM CropMonitor.{view} WHERE fc_extension = '{fc}' AND date >= DATEADD(DAY, -30, GETDATE()) group by fc_extension, date order by date")
      df = pd.concat([df, res])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Latest available data

# COMMAND ----------

df2 = pd.DataFrame()
for fc in fc_extensions:
  for indic_name, view_specs in indicators_views.items():
    for view_spec in view_specs:
      view = view_spec['view']
      indic_abr = view_spec['name']
      source_code = view_spec.get('source')
      source_filter = f"and SourceCode = '{source_code}'" if source_code else ''
      res = sql.sql_query(DB, f'''
                          Select fc_extension, '{indic_abr}' as indic_abr, max(date) as date FROM CropMonitor.{view} WHERE fc_extension = '{fc}' {source_filter} group by fc_extension''')
      df2 = pd.concat([df2, res])
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualing data

# COMMAND ----------

for fc in fc_extensions:
  for indic_name, view_specs in indicators_views.items():
    df3 = pd.DataFrame()
    end = date(year=9999, month=12, day=31)
    start = date(year=1, month=1, day=1)
    for view_spec in view_specs:
      view = view_spec['view']
      indic_abr = view_spec['name']
      column = view_spec['column']
      source_code = view_spec.get('source')
      source_filter = f"and SourceCode = '{source_code}'" if source_code else ''
      indic_latest_date = df2[(df2['fc_extension'] == fc) & (df2['indic_abr'] == indic_abr)]
      start = indic_latest_date['date'].iloc[0]
      start = min((start - timedelta(days=10)), end)
      end = indic_latest_date['date'].iloc[0]
      end = min((start + timedelta(days=30)), end) # Just look ahead 30 days
      res = sql.sql_query(
        DB, f'''Select AdmId, date, {column} as value FROM CropMonitor.{view} WHERE fc_extension = '{fc}' and date >= '{start.strftime("%Y-%m-%d")}' AND date <= '{end.strftime("%Y-%m-%d")}' {source_filter}''')
      res['fc_extension'] = fc
      res['indic_abr'] = indic_abr
      df3 = pd.concat([df3, res])
      
    if not df3.empty:
      # We are choosing AdmIds at random, but you can easily hardcode one of your preference
      random_adm_id = random.choice(df3['AdmId'].tolist())
      df3 = df3[df3['AdmId'] == random_adm_id]

      plt.figure(figsize=(10, 6))
      for indic_abr in df3['indic_abr'].unique():
          subset = df3[df3['indic_abr'] == indic_abr].sort_values('date')
          plt.plot(subset['date'], subset['value'], label=indic_abr)
      plt.title(f'{indic_name} for {fc} - AdmId {random_adm_id}')
      plt.legend()
      plt.grid(True)
      plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check issues for tmin >= tavg >= tmax

# COMMAND ----------

df4 = pd.DataFrame()
for fc in fc_extensions:
  res = sql.sql_query(
        DB, f'''
        Select fc_extension, AdmId, Date FROM CropMonitor.EcmwfEra5landTemperatureDailyView
        WHERE fc_extension = '{fc}' and (MinTemperature >= AvgTemperature or AvgTemperature >= MaxTemperature)
        UNION
        Select fc_extension, AdmId, Date FROM CropMonitor.EcmwfEra5landTemperatureDailyForecastView
        WHERE fc_extension = '{fc}' and DateId >= {datetime.today().strftime("%Y%m%d")}
        and (MinTemperature >= Temperature or Temperature >= MaxTemperature) ''')
  df4 = pd.concat([df4, res])
print('No issues for tmin >= tavg >= tmax') if df4.empty else display(df4) 
