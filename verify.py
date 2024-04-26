import fastavro
from glob import glob

for file_name in glob("./database2/*.avro"):
  print(f"Verify file: {file_name}")
  with open(file_name, "rb") as database:
    avro_reader = fastavro.reader(database)
    schema = avro_reader.writer_schema
    records = list(avro_reader)

  print("Schema: ")
  print(schema["fields"])
  print("Records: ")
  for x in records[0:4]:
    print(x)