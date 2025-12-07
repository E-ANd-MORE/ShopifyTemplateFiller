import pandas as pd
import os

def transform_csv(input_folder, input_filename="s.csv", output_filename="s_fixed.csv"):
    # Ruta completa
    input_path = os.path.join(input_folder, input_filename)
    output_path = os.path.join(input_folder, output_filename)

    # Leer CSV
    df = pd.read_csv(input_path)

    # Columnas esperadas en formato final
    target_columns = ['PIM | Brand', 'UPC Code', 'English Description', 'COST']

    missing = [col for col in target_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en el origen: {missing}")

    # Crear nuevo dataframe con solo columnas necesarias
    df_fixed = df[target_columns].copy()

    # Guardar a s_fixed.csv
    df_fixed.to_csv(output_path, index=False)
    print(f"Archivo transformado guardado en: {output_path}")


# Ejemplo de uso:
# transform_csv("input")  # si el folder se llama 'input'
transform_csv("input")
