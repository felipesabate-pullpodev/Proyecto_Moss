from prefect import flow
from bsale.components.documentos.carga_diaria_bsale_documentos import carga_diaria_flow
from bsale.components.documentos.dbt_run import dbt_flow

@flow
def main_flow():
    # Paso 1: Ejecutar la carga diaria
    print("=== Ejecutando carga diaria ===")
    carga_diaria_flow()

    # Paso 2: Ejecutar modelos dbt
    print("=== Ejecutando modelos dbt ===")
    dbt_flow()

    print("=== Flujo principal completado ===")

if __name__ == "__main__":
    main_flow()
