#### Add `Minimal Compute Policy` To Lakeflow Connect Ingestion Gateway Pipeline
```bash
databricks pipelines list-pipelines --output json

databricks pipelines update 7ac1fd16-b92f-45d3-b0e2-a3330d0f3b73 --json '{
  "name": "gw_ingestion",
  "catalog": "ws_dbxproject_7405606677876678",
  "schema": "00_landing",
  "gateway_definition": {
    "connection_name": "conn_restaurantops",
    "gateway_storage_catalog": "ws_dbxproject_7405606677876678",
    "gateway_storage_schema": "00_landing",
    "gateway_storage_name": "gw_ingestion"
  },
  "clusters": [
    {
      "label": "default",
      "policy_id": "001FDEA0FF111B45",
      "apply_policy_default_values": true
    }
  ],
  "continuous": true
}'
```

#### CDC Changes
```sql
UPDATE customers
SET city='Abu Dhabi'
WHERE customer_id='CUST-10000';

INSERT INTO dbo.menu_items (restaurant_id, item_id, name, category, price, ingredients, is_vegetarian, spice_level)
VALUES ('REST-AUH-001','ITEM-999','Samosa (2 pcs)','Starter',18.49,'Potato, Peas, Spices, Pastry',1,'Medium');
```

### SDP Event Log
```sql
SELECT
  timestamp,
  details:flow_definition.output_dataset,
  details:flow_progress.status,
  details:planning_information
FROM event_log(TABLE(`03_gold`.d_customer_360))
WHERE details:planning_information IS NOT NULL
ORDER BY timestamp DESC
LIMIT 10;
```