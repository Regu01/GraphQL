import json

input_file = "device_introspection.json"
output_file = "device_query.graphql"

# Charger le JSON d'introspection
with open(input_file, "r") as f:
    data = json.load(f)

# Extraire les noms de champs
fields = data["data"]["__type"]["fields"]
field_names = [f["name"] for f in fields]

# Construire la chaîne des champs avec indentations
fields_str = "\n        ".join(field_names)

# Construire la requête GraphQL complète
graphql_query = f"""query {{
  devices(first: 5) {{
    edges {{
      node {{
        {fields_str}
      }}
    }}
  }}
}}"""

# Sauvegarder directement dans un fichier .graphql
with open(output_file, "w", encoding="utf-8") as f:
    f.write(graphql_query)

print(f"Requête GraphQL complète générée dans {output_file}")
