import fastavro
from glob import glob

for file_name in glob("./database/*.avro"):
  print(f"processing file: {file_name}")
  with open(file_name, "rb") as database:
    avro_reader = fastavro.reader(database)
    schema = avro_reader.writer_schema
    records = list(avro_reader)

  for idx, field in enumerate(schema["fields"]):
    if "." in field["name"]:
      field["name"] = field["name"].replace(".", "_")
    if " " in field["name"]:
      field["name"] = field["name"].replace(" ", "_")
    if field["name"]=="number":
      field["name"]="Number"
      schema["fields"][idx]=field

  for idx, field in enumerate(schema["fields"]):
    if field["name"] == "ingestion_date":
      schema["fields"].pop(idx)

  for idx, field in enumerate(schema["fields"]):
    print(f"{idx} : {field}")

  for x in records:
    # removal ingestion date
    x.pop("ingestion_date")
    # fix mega naming
    if "Mega" in x["Name"]:
      name = x["Name"]
      x["Name"] = "Mega"+name.split("Mega", 1)[1]
    try:
      x["Type_1"] = x.pop("Type 1")
      x["Type_2"] = x.pop("Type 2")
      x["Sp_Atk"] = x.pop("Sp.Atk")
      x["Sp_Def"] = x.pop("Sp.Def")
    except:
      None
    x["Number"] = x.pop("number")
  
  for x in records[0:5]:
    print(x)

  print(f"Saviung file: {file_name}")
  with open(file_name+"new", "wb") as database:
    fastavro.writer(database,schema,records)
    