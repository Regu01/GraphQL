import json

# Fichier d'entrée contenant la réponse introspection
input_file = "device_introspection.json"
output_file = "device_query.json"

# Charger le JSON
with open(input_file, "r") as f:
    data = json.load(f)

# Extraire les noms de champs
fields = data["data"]["__type"]["fields"]
field_names = [f["name"] for f in fields]

# Construire la chaîne des champs
fields_str = "\n        ".join(field_names)

# Construire la requête GraphQL complète
graphql_query = f"""
query {{
  devices(first: 5) {{
    edges {{
      node {{
        {fields_str}
      }}
    }}
  }}
}}
"""

# Sauvegarder dans un fichier JSON
output_data = {"query": graphql_query}

with open(output_file, "w") as f:
    json.dump(output_data, f, indent=2)

print(f"Requête GraphQL complète générée dans {output_file}")
