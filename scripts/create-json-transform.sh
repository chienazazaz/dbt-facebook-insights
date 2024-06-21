db=$(grep 'json_transform_schema:' ./.venv/dbt_project.yml | tail -n1)
db=${db//*json_transform_schema: /}
if [ "$USER"=="lechien" ]; then
  echo "running on lechien"
  make create-json-transform
  exit 1
else
  dbt run-operation create_json_transform --args '{"schema":"$db"}'
  exit 1
fig