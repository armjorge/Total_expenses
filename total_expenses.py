import pandas as pd
import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import numpy as np

print("\n*******************************\nRecuerda que si bajas archivos al corte, se guardan con el número de mes de la fecha de corte, por ejemplo fecha corte 06 jul 2024, y estás a 7 de julio, el CVS es el correspondiente a 2024 07.csv\n*******************************\n")

# Define the scope
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
# Add your service account file
creds = ServiceAccountCredentials.from_json_keyfile_name('./armjorgeSheets.json', scope)  # Ensure the correct path
# Authorize the client sheet
client = gspread.authorize(creds)
spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1dYt-ykqez6wBSpYo22WXzkyBgHkJBJwjPb5ciIECcro/edit?gid=0#gid=0')

# Get today's date information
today = datetime.datetime.now()
month_today = today.strftime('%m')  # Month as two digits string
day_today = today.strftime('%d')    # Day as two digits string
year_today = today.strftime('%Y')   # Year as four digits string
df_months_cutoff = pd.read_excel('./Core/df_fechas_corte.xlsx')

def fechas_de_corte():
    global df_months_cutoff

    # Ensure 'Fecha_corte' and 'Start_day' columns are in datetime format
    df_months_cutoff['Fecha_corte'] = pd.to_datetime(df_months_cutoff['Fecha_corte'])
    df_months_cutoff['Start_day'] = pd.to_datetime(df_months_cutoff['Start_day'])

    # Sort the DataFrame from newer to older based on 'Start_day'
    df_months_cutoff = df_months_cutoff.sort_values(by='Start_day', ascending=False)

    # Get the newest 'Start_day' month and year
    newest_start_day = df_months_cutoff.iloc[0]['Start_day']
    newest_start_month = newest_start_day.month
    newest_start_year = newest_start_day.year

    # Compare today month with the newest 'Start_day' month
    if int(month_today) > newest_start_month or (int(month_today) == 1 and newest_start_month == 12):
        # Input the 'Día de corte' from the user
        dia_de_corte = int(input("Ingrese el Día de corte: "))

        # Create a new row
        new_fecha_corte = datetime.datetime(int(year_today), int(month_today), dia_de_corte)
        new_start_day = datetime.datetime(int(year_today), int(month_today), 1)
        new_mes = year_today + "-" + month_today

        new_row = {
            'Fecha_corte': new_fecha_corte,
            'Start_day': new_start_day,
            'Mes': new_mes
        }

        # Append the new row to the DataFrame using pd.concat
        df_months_cutoff = pd.concat([df_months_cutoff, pd.DataFrame([new_row])], ignore_index=True)

    elif int(month_today) == newest_start_month and int(year_today) == newest_start_year:
        print(f"Mes corriente ya cargado con la fecha de corte {df_months_cutoff.iloc[0]['Fecha_corte']}")

    else:
        print("Revisa el excel, parece que tienes carpetas de más")

    # Write the updated DataFrame back to Excel
    with pd.ExcelWriter('./Core/df_fechas_corte.xlsx', engine='openpyxl') as writer:
      df_months_cutoff.to_excel(writer, index=False)

    return df_months_cutoff

def filter_and_merge_files(df_meses_y_fcorte):
    global today

    # Filter rows where 'Fecha_corte' is less than today
    df_filtered = df_meses_y_fcorte[df_meses_y_fcorte['Fecha_corte'] < today]
    ## Aquí tenemos que preguntar si ya tenemos fecha de corte. 
    # Generate file paths
    file_list = [f'./Repositorios TC al corte/{mes}.csv' for mes in df_filtered['Mes']]

    # Print the file list filtered
    print("Files to be merged:")
    for file in file_list:
        print(file)

    # Merge the listed .csv files
    df_list = []
    for file, mes in zip(file_list, df_filtered['Mes']):
        df_temp = pd.read_csv(file)
        df_temp['Source_file'] = mes + '.csv'
        df_list.append(df_temp)

    df_merged = pd.concat(df_list, ignore_index=True)
    #df_merged['Concepto'] = df_merged['Concepto'].str.replace(r'[^\w\s]', '', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()

    # Ensure 'Fecha' column is in datetime format
    if 'Fecha' in df_merged.columns:
        df_merged['Fecha'] = pd.to_datetime(df_merged['Fecha'], errors='coerce', dayfirst=True)
    df_merged['Source'] = 'Corte'
    # Save the merged DataFrame to Excel
    with pd.ExcelWriter('./Excel_global.xlsx', engine='openpyxl') as writer:
      df_merged.to_excel(writer, sheet_name='Desglose', index=False)

    return df_merged

def gastos_despues_del_corte(df_meses_y_fcorte):
    global today

    # Get the newest 'Fecha_corte' and corresponding 'Mes'
    newest_fecha_corte = df_meses_y_fcorte['Fecha_corte'].max()
    newest_mes = df_meses_y_fcorte.loc[df_meses_y_fcorte['Fecha_corte'] == newest_fecha_corte, 'Mes'].values[0]

    # Define folder to process
    folder_to_process = f'./{newest_mes}'

    # Check if folder exists
    if not os.path.exists(folder_to_process):
        os.makedirs(folder_to_process)
        print(f"Carpeta de mes no encontrada, creando {folder_to_process}")

    # Search for CSV files with the prefix TC in the folder
    tc_files = [f for f in os.listdir(folder_to_process) if f.startswith('TC') and f.endswith('.csv')]

    if not tc_files:
        print(f"Ve a tu banco móvil, descarga el csv después del {newest_fecha_corte}, agrega el prefijo TC y guárdalo en la carpeta {folder_to_process}, luego vuelve a intentar")
        return None

    # Get the most recent TC file
    tc_files.sort(key=lambda x: os.path.getmtime(os.path.join(folder_to_process, x)), reverse=True)
    csv_despues_del_corte = tc_files[0]

    print(f"{csv_despues_del_corte} es el archivo más reciente que empieza con TC, en la carpeta {folder_to_process}")

    # Load the most recent TC file
    df_cargos_despues_del_corte = pd.read_csv(os.path.join(folder_to_process, csv_despues_del_corte), encoding='latin1')

    # Add 'Source_file' column
    df_cargos_despues_del_corte['Source_file'] = csv_despues_del_corte
    df_cargos_despues_del_corte['Fecha'] = pd.to_datetime(df_cargos_despues_del_corte['Fecha'], format='%d/%m/%Y')
    df_cargos_despues_del_corte['Source'] = 'Después del corte'
    # Search for CSV files with the prefix MSI in the folder
    msi_files = [f for f in os.listdir(folder_to_process) if f.startswith('MSI') and f.endswith('.csv')]

    if not msi_files:
        print(f"Ve a tu banco móvil, descarga el csv Compras a plazo, agrega el prefijo MSI y guárdalo en la carpeta {folder_to_process}, luego vuelve a intentar")
        return None

    # Get the most recent MSI file
    msi_files.sort(key=lambda x: os.path.getmtime(os.path.join(folder_to_process, x)), reverse=True)
    msi_despues_del_corte = msi_files[0]

    print(f"{msi_despues_del_corte} es el archivo más reciente que empieza con MSI, en la carpeta {folder_to_process}")

    # Load the most recent MSI file
    df_brut_msi = pd.read_csv(os.path.join(folder_to_process, msi_despues_del_corte), encoding='latin1')
    df_brut_msi['Fecha de operación'] = pd.to_datetime(df_brut_msi['Fecha de operación'], format='%d/%m/%Y')
    df_brut_msi['Fecha de operación'] = df_brut_msi['Fecha de operación'].apply(
        lambda x: x.replace(month=today.month)
    )

    def duplicate_rows_with_incremented_months(df):
        new_rows = []

        for index, row in df.iterrows():
            count = int(row['Pagos pendientes'].split('/')[0])
            original_suffix = row['Pagos pendientes'].split('/')[1]
            for i in range(count):
                new_row = row.copy()
                new_row['Fecha de operación'] = new_row['Fecha de operación'] + pd.DateOffset(months=i+1)
                new_row['Pagos pendientes'] = f"{count-i-1:02d}/{original_suffix}"
                new_rows.append(new_row)

        duplicated_df = pd.DataFrame(new_rows, columns=df.columns)
        return pd.concat([df, duplicated_df], ignore_index=True).sort_values(by='Fecha de operación')


    df_brut_msi = duplicate_rows_with_incremented_months(df_brut_msi)
    #print(df_brut_msi)

    # Process 'Pagos pendientes' column
    rows_to_remove = []
    for index, row in df_brut_msi.iterrows():
        pagos_pendientes = row['Pagos pendientes']
        if '/' in pagos_pendientes:
            pending, total = map(int, pagos_pendientes.split('/'))
            if pending == total:
                print(f"Fila removida porque no entra hasta el siguiente corte: Concepto: {row['Concepto']}, Mensualidad: {row['Mensualidad']}")
                rows_to_remove.append(index)

    df_brut_msi.drop(rows_to_remove, inplace=True)

    # Create df_meses_sin_intereses
    df_meses_sin_intereses = pd.DataFrame()
    df_meses_sin_intereses['Fecha'] = df_brut_msi['Fecha de operación']
    df_meses_sin_intereses['Concepto'] = df_brut_msi['Concepto']
    df_meses_sin_intereses['Abono'] = 0
    df_meses_sin_intereses['Cargo'] = df_brut_msi['Mensualidad']
    df_meses_sin_intereses['Tarjeta'] = "TC a meses sin intereses"
    df_meses_sin_intereses['Source_file'] = msi_despues_del_corte
    df_meses_sin_intereses['Source'] = 'Meses sin intereses'
    # Merge df_meses_sin_intereses with df_cargos_despues_del_corte
    df_combined = pd.concat([df_meses_sin_intereses, df_cargos_despues_del_corte], ignore_index=True)
    df_combined['Concepto'] = df_combined['Concepto'] \
      .str.replace(r'\t', ' ', regex=True) \
      .str.replace(r'[^\w\s]', '', regex=True) \
      .str.replace(r'\s+', ' ', regex=True) \
      .str.strip()
    output_path = os.path.join(folder_to_process, "Cargosdespuesdelcorte.xlsx")
    df_combined.to_excel(output_path, index=False)
    print(f"Datos guardados en {output_path}")
    return df_combined

# Update cutoff dates if needed
df_meses_y_fcorte = fechas_de_corte()
# Filter and merge files
df_meses_con_corte = filter_and_merge_files(df_meses_y_fcorte)
df_meses_con_corte['Fecha'] = pd.to_datetime(df_meses_con_corte['Fecha'], format='%d/%m/%Y')
df_cargos_MSI_y_despuesdel_corte  = gastos_despues_del_corte(df_meses_y_fcorte)

df_cargos_post_y_al_corte = pd.concat([df_meses_con_corte, df_cargos_MSI_y_despuesdel_corte], ignore_index=True)

# Group by month and year
df_group_by_mes_año = df_cargos_post_y_al_corte.groupby(df_cargos_post_y_al_corte['Fecha'].dt.to_period('M')).agg({'Abono': 'sum', 'Cargo': 'sum'}).reset_index()
df_group_by_mes_año['Fecha'] = df_group_by_mes_año['Fecha'].dt.to_timestamp()

# Filter for the current month and group by day, month, and year
current_month = today.month
current_year = today.year
print(f"Generando df del mes en curso {current_month} año {current_month}")

df_current_month = df_cargos_post_y_al_corte[(df_cargos_post_y_al_corte['Fecha'].dt.month == current_month) & (df_cargos_post_y_al_corte['Fecha'].dt.year == current_year)]
df_group_by_dia_mes_actual = df_current_month.groupby(df_current_month['Fecha'].dt.to_period('D')).agg({'Abono': 'sum', 'Cargo': 'sum'}).reset_index()
df_group_by_dia_mes_actual['Fecha'] = df_group_by_dia_mes_actual['Fecha'].dt.to_timestamp()


# Combine all dataframes into a dictionary for easy reference
df_informacion_actualizada = {
    'df_cargos_post_y_al_corte': df_cargos_post_y_al_corte,
    'df_group_by_mes_año': df_group_by_mes_año,
    'df_group_by_dia_mes_actual': df_group_by_dia_mes_actual
}

# Function to clear and update Google Sheets
def update_google_sheet(sheet_name, dataframe):
    gsheet = spreadsheet.worksheet(sheet_name)
    gsheet.clear()
    set_with_dataframe(gsheet, dataframe)

# Update the sheets with dataframes from df_informacion_actualizada
update_google_sheet('TC_2024', df_informacion_actualizada['df_cargos_post_y_al_corte'])
update_google_sheet('TC_month_year', df_informacion_actualizada['df_group_by_mes_año'])
update_google_sheet('TC_dia_corriente', df_informacion_actualizada['df_group_by_dia_mes_actual'])

print("\n*******************************\nDataframe actualizado en el Google Sheet\n*******************************\n")


""" 
Hasta aquí se genera el dataframe, ahora se procede a generar el txt para obsidian. 
"""

# @title
import pandas as pd
from datetime import datetime
import numpy as np

# Assuming df_cargos_post_y_al_corte, df_group_by_mes_año, and df_group_by_dia_mes_actual are already defined

# Function to generate the markdown table for a given month and year
def generate_monthly_summary(df, year, month):
    # Filter data for the specified month and year
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    mask = (df['Fecha'].dt.year == year) & (df['Fecha'].dt.month == month)
    df_filtered = df.loc[mask]

    # Group by day and sum 'Cargo'
    daily_sums = df_filtered.groupby(df_filtered['Fecha'].dt.day)['Cargo'].sum().reindex(range(1, 32), fill_value=0).round(0)

    # Create the markdown table
    weeks = []
    week = ['Semana 1'] + [None] * 7 + [0]  # Header for the first week
    current_week = 1

    for day in range(1, 32):
        try:
            date = datetime(year, month, day)
        except ValueError:
            break  # Exit if the day is not valid for the month

        week[date.weekday() + 1] = f"${daily_sums[day]:,.0f}" if daily_sums[day] != 0 else "$0"
        week[-1] += daily_sums[day]

        if date.weekday() == 6 or day == daily_sums.index[-1]:  # End of the week or end of the month
            week[-1] = f"${int(week[-1]):,}"  # Ensure the total is rounded and formatted with commas            
            weeks.append(week)
            current_week += 1
            week = [f'Semana {current_week}'] + [None] * 7 + [0]

    # Fill the remaining week with $0
    for i in range(1, 8):
        if week[i] is None:
            week[i] = "$0"

    if week[-1] != 0:
        weeks.append(week)

    # Convert to markdown table format
    table = "|Sem|Dom|Lun|Mar|Mie|Jue|Vie|Sáb|Total|\n"
    table += "|---|---|---|---|---|---|---|---|---|\n"
    for week in weeks:
        table += "|" + "|".join(map(str, week)) + "|\n"

    return table

# Iterate through each month in the data and generate summaries
def generate_summaries(df):
    summaries = ""
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    start_date = df['Fecha'].min()
    end_date = df['Fecha'].max()

    current_year = start_date.year
    current_month = start_date.month

    while current_year < end_date.year or (current_year == end_date.year and current_month <= end_date.month):
        summaries += f"## {datetime(current_year, current_month, 1).strftime('%B %Y')}\n"
        summaries += generate_monthly_summary(df, current_year, current_month)
        summaries += "\n"

        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1

    return summaries

# Main process
df = df_informacion_actualizada['df_cargos_post_y_al_corte']
summaries = generate_summaries(df)

# Save the summaries to a text file
file_path = './Obsidiantables.txt'
with open(file_path, 'w') as file:
    file.write(summaries)

print("Markdown summaries have been saved to:", file_path)
