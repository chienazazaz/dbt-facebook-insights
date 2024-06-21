# GA4 DBT Package 

This [dbt](https://www.getdbt.com/) package connects to Facebook data exported using this custom extractor [here](https://github.com/chienazazaz/fb-insights)

Features include:


# Models


# Installation & Configuration

## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile and a BigQuery GCP instance available with GA4 event data loaded. Source data is defined using the `project` and `dataset` variables below. The `static_incremental_days` variable defines how many days' worth of data to reprocess during incremental runs. 

```
vars:
  dbt_fb_insights:
    source_dataset: "my_facebook_dataset" # Project that contains raw facebook data
    lookback_period: 3 # Number of days to scan and reprocess on each run
```

## Optional Variables

### Enable interactive & ecommerce actions
These variables is used to extract interactive actions (page engagement, link clicks,...etc) and ecommerce actions (purchase, offline_conversion,...etc) number of occurence and its value. The default value is true, if you want to disable it, set it to false.
```
vars:
  dbt_fb_insights:
    is_interactive_actions_enabled: false
    is_ecommerce_actions_enabled: false
```

### Additional interactive & ecommerce actions
These variables is used to extract additional interactive actions and ecommerce actions. Interactive actions mean actions that do not have conversion value that will attribute to your ad, you can switch actions between both of these variables. The default value is empty, if you want to add more actions, you can add it to the list. The list of known action types can be found [here](https://developers.facebook.com/docs/marketing-api/reference/ads-action-stats/), under `action_type` section. For example:
```
  additional_ad_interact_actions:
    - onsite_conversion.messaging_order_created_v2
    - onsite_conversion.total_messaging_connection
    - onsite_conversion.messaging_first_reply
  additional_ad_ecommerce_actions:
    - onsite_web_app_add_to_cart
    - onsite_web_add_to_cart
```

### Transform JSON field UDF schema
This variable used to create a function named json_transform which take a JSON field in form `{key_name:value}` and tranform it to an array of JSON object to `{key:key_name,value:value}`. This comes in handy where the data structure is not normalized and you want to extract the value of the key.
To automatically create the function, run the following command:
```bash
bash ./scripts/create-json-transform.sh
```

or run
```
dbt run-operation create_json_transform '{schema:"my_schema"}'
``` 
The function will be created in the defined schema.

```yml
vars:
  dbt_fb_insights:
    json_transform_schema: "my_schema"
```

# Connecting to BigQuery

This package assumes that BigQuery is the source of your GA4 data. Full instructions for connecting DBT to BigQuery are here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile

The easiest option is using OAuth with your Google Account. Summarized instructions are as follows:
 
1. Download and initialize gcloud SDK with your Google Account (https://cloud.google.com/sdk/docs/install)
2. Run the following command to provide default application OAuth access to BigQuery:

```
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```