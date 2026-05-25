"""Configuración de los recursos, en este script no se acepta nada de python"""

REQS_FILE = "../reqs.txt"
REPOSITORY = "fraud_prevention"

PATHS = {"qas":"fraud-prevention",
         "prd":"data-ops-fraud-prevention"}

TABLES = {REPOSITORY:{
                "data_vault":["hub_client",
                            "sat_client_attrs",
                            "sat_client_addr",
                            "sat_client_flags",
                            "sat_client_beneficiaries",
                            "sat_client_document_data",
                            "sat_client_onb_attrs",
                            "sat_client_onb_flags",
                            "sat_client_kyc",
                            "sat_client_alias"],
                "star_schema":["current_account_x",
                            "dim_client_x"]},
        }

# Finite Incantatem

