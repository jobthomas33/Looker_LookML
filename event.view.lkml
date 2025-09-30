view: events {
  # Friendly name displayed to users in the Field Picker.
  label: "Events - Overview"

  # --- START:  Derived Table  Definition ---
  derived_table: {
    # This SQL defines a derived table. Its primary function is to FLATTEN (unnest) multiple 
    # nested product attribute fields within the raw Rudderstack event data, 
    # multiplying rows so that each product item gets its own row for analysis.
    sql:
      SELECT
        # Select all columns from the base table (e), EXCEPT the original nested JSON 
        # fields which are being processed and replaced below.
        e.* except(product_id,currency,price,list_price,sales_price,size,quantity,product_name,color)

        # --- Flattening/Coalescing Logic for Array Fields ---
        # COALESCE prioritizes the unnested value (from the LEFT JOIN UNNEST below) 
        # and falls back to the original value (e.product_id) if the UNNEST results 
        # in no row, ensuring data is captured whether it was an array or a single value.
        ,coalesce(JSON_EXTRACT_SCALAR(product_id,"$"),e.product_id) as product_id
        ,coalesce(JSON_EXTRACT_SCALAR(currency,"$"),e.currency) as currency
        # Casting extracted JSON strings to numeric types for calculations
        ,SAFE_CAST(coalesce(JSON_EXTRACT_SCALAR(price,"$"),e.price) as FLOAT64) as price
        ,SAFE_CAST(coalesce(JSON_EXTRACT_SCALAR(list_price,"$"),e.list_price) as FLOAT64)  as list_price
        ,SAFE_CAST(coalesce(JSON_EXTRACT_SCALAR(sales_price,"$"),e.sales_price) as FLOAT64)  as sales_price
        ,coalesce(JSON_EXTRACT_SCALAR(size,"$"),e.size) as size
        ,SAFE_CAST(coalesce(JSON_EXTRACT_SCALAR(quantity,"$"),e.quantity) as INTEGER) as quantity
        ,coalesce(JSON_EXTRACT_SCALAR(product_name,"$"),e.product_name) as product_name
        ,coalesce(JSON_EXTRACT_SCALAR(color,"$"),e.color) as color

        ,e._PARTITIONDATE as partition_date

        # Generates a row number per unique event ID. This is CRITICAL for creating a 
        # unique primary key after the row-multiplying UNNEST joins.
        ,Row_number() over (Partition by e.id order by product_id) as product_rownum

        # --- Extracting Nested Item Properties from 'all_props_ecommerce_items' ---
        # These fields are extracted from the "all_props_ecommerce_items_unnest" alias 
        # created by the final UNNEST join below.
        ,JSON_VALUE(all_props_ecommerce_items_unnest, '$.currency') AS currency_item
        ,JSON_VALUE(all_props_ecommerce_items_unnest, '$.discount') AS discount_item
        ,JSON_VALUE(all_props_ecommerce_items_unnest, '$.index') AS index_item
        ,JSON_VALUE(all_props_ecommerce_items_unnest, '$.item_id') AS item_id
        ,JSON_VALUE(all_props_ecommerce_items_unnest, '$.item_name') AS item_name
        ,SAFE_CAST(JSON_VALUE(all_props_ecommerce_items_unnest, '$.price') as FLOAT64) AS price_item
        ,SAFE_CAST(JSON_VALUE(all_props_ecommerce_items_unnest, '$.quantity') as FLOAT64) AS quantity_item


      FROM
        # Source table: raw Rudderstack events in BigQuery
        `project_rudderstack.rudderstack.events` e  

      # --- UNNEST Joins to Flatten Arrays ---
      # LEFT JOIN UNNEST creates one row for every element in the JSON array, 
      # using WITH OFFSET to preserve ordering (though not used in the SELECT).
      left join unnest(JSON_EXTRACT_ARRAY(product_id, '$')) product_id WITH OFFSET AS offset_product_id
      left join unnest(JSON_EXTRACT_ARRAY(currency, '$')) currency WITH OFFSET AS offset_currency
      left join unnest(JSON_EXTRACT_ARRAY(price, '$')) price WITH OFFSET AS offset_price
      left join unnest(JSON_EXTRACT_ARRAY(list_price, '$')) list_price WITH OFFSET AS offset_list_price
      left join unnest(JSON_EXTRACT_ARRAY(sales_price, '$')) sales_price WITH OFFSET AS offset_sales_price
      left join unnest(JSON_EXTRACT_ARRAY(size, '$')) size WITH OFFSET AS offset_size
      left join unnest(JSON_EXTRACT_ARRAY(quantity, '$')) quantity WITH OFFSET AS offset_quantity
      left join unnest(JSON_EXTRACT_ARRAY(product_name, '$')) product_name WITH OFFSET AS offset_product_name
      left join unnest(JSON_EXTRACT_ARRAY(color, '$')) color WITH OFFSET AS offset_color
      # Final UNNEST for the general e-commerce items array
      left join UNNEST(JSON_EXTRACT_ARRAY(all_props_ecommerce_items, '$')) AS all_props_ecommerce_items_unnest
    ;;
  }
  # --- END:  Derived Table Definition ---

  # Access Grant: Limits visibility of this view to users who possess these access grants.
  required_access_grants: [can_view_event_analytics,can_view_internal_only_data]


  # --- START: Dimensions ---

  # Primary Rudderstack identifier for the event.
  dimension: id {
    label: "Message ID"
    description: "Rudderstack unique identifier for the event."
    type: string
    sql: ${TABLE}.id ;;
  }

  # Primary Key: Ensures every row in the flattened PDT is uniquely identified.
  # Concatenation of ID, Product ID, and the row number generated in the DT SQL.
  dimension: primary_key {
    hidden: yes
    primary_key: yes
    type: string
    sql: CONCAT(${id},COALESCE(${product_id},''), ${TABLE}.product_rownum) ;;
  }

  # Dimension Group for BigQuery partitioning date.
  dimension_group: _partitiondate {
    label: "Data Load"
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.partition_date ;;
  }

  # Anonymous ID: Tracks unidentified user behavior (pre-login).
  dimension: anonymous_id {
    view_label: "Events - Overview"
    description: "Unique identifier to track unidentified users, scoped per retailer and per device. Source: Rudderstack SDK."
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  # Channel: Source of the event (mobile, web, server).
  dimension: channel {
    type: string
    description: "Identifies the source of the event. Permitted values are mobile, web, server and source. Source: Rudderstack SDK."
    sql: ${TABLE}.channel ;;
  }

  # --- Dimension with Liquid for Dynamic View Label ---
  # Liquid is used here to conditionally format the view_label. 
  # If the current Explore is not named 'events', it prefixes the view_label.
  dimension: color {
    view_label: "{% if _explore._name != 'events' %}Events - {% endif %}Product Details"
    group_label:"Product"
    type: string
    sql: ${TABLE}.color ;;
  }

  # ... (Other context dimensions are included, many are hidden by default) ...

  # Order ID: Coalesces with 'all_props_pre_order_code' for robust pre-order identification.
  dimension: order_id {
    label: "Pre-order ID"
    description: "X unique identifier for the shopping cart (pre-order ID available within checkout URL). Source: dataLayer."
    type: string
    sql: COALESCE(${TABLE}.all_props_pre_order_code,${TABLE}.order_id) ;;
  }

  # Order Code: Populated only for completed, confirmed orders.
  dimension: order_code {
    description: "Unique reference number for the order. Populated for confirmed orders, empty if not."
    type: string
    sql: ${TABLE}.order_code ;;
  }

  # ... (timeframes and base product dimensions follow) ...

  # --- E-commerce Item-Level Properties (Extracted from JSON in DT) ---
  dimension:  currency_item {
    hidden: yes
    group_label: "Items"
    view_label:  "Events- Properties"
    label: "Currency (Item level)"
    type: string
    sql: ${TABLE}.currency_item ;;
  }

  dimension:  discount_item {
    group_label: "Items"
    view_label:  "Events- Properties"
    label: "Discount"
    type: string
    sql: ${TABLE}.discount_item ;;
  }
  # ... (rest of item-level dimensions) ...

  # --- Measures Section ---
  ######Measures

  # Base count measure, hidden, often used as the target for drill-downs.
  measure: count {
    hidden: yes
    type: count
    drill_fields: [detail*]
  }

  # Total count of all distinct Rudderstack events (using the message ID).
  measure: total_events {
    group_label: "Rudderstack Events and CVR"
    view_label: "Events - Overview"
    description: "Total count of all Rudderstack events."
    type: count_distinct
    sql: ${id} ;;
  }

  # Total Price: Simple sum over the price dimension.
  measure: price_value {
    view_label: "{% if _explore._name != 'events' %}Events - {% endif %}Product Details"
    group_label:"Product"
    label: "Total Price"
    type: sum
    sql: ${price} ;;
    value_format: "#,##0.00;-#,##0.00"
  }

  # --- Distinct Sum Measures (Crucial for Flattened Data) ---
  # These measures use `sum_distinct` combined with `sql_distinct_key: ${id}`.
  # This pattern ensures values like shipping, tax, or subtotal (which are 
  # constant across all product rows of a single event) are only counted ONCE per 
  # original event ID, preventing over-aggregation from the UNNEST operation.
  measure: shipping_value {
    view_label: "{% if _explore._name != 'events' %}Events - {% endif %}Product Details"
    group_label:"Product"
    label: "Total Shipping"
    type: sum_distinct
    sql_distinct_key: ${id} ;;
    sql: ${shipping} ;;
    value_format: "#,##0.00;-#,##0.00"
  }

  # ... (subtotal_value, tax_value, total_value follow the sum_distinct pattern) ...

  # Drill Set: Defines the set of dimensions displayed when a user clicks 'Drill Into' 
  # on any measure associated with this set.
  set: detail {
    fields: [
      id,
      context_library_name,
      product_name,
      event_name,
      context_app_name
    ]
  }

  ############################### Rudderstack Funnel Metrics ######################################################
  # These measures define the steps of a conversion funnel, filtered by specific event_name 
  # values and counted across different granularity levels (ID, Shopper, Customer, Pre-order).

  ## 1. Checkout Load (Filter: "Checkout Started")
  measure: checkout_load {
    group_label: "Rudderstack Events and CVR"
    view_label: "Events - Overview"
    label: "1.Checkout Load"
    description: "Total count of Checkout Started events (Event ID level)."
    type:  count_distinct
    sql: ${id} ;;
    filters: {
      field: event_name
      value: "Checkout Started"
    }
  }

  # Funnel step counted by X_analytics_id (Shopper identifier).
  measure: checkout_load_shopper {
    hidden: yes
    group_label: "Rudderstack Events and CVR - Shopper level"
    description: "Distinct count of Checkout Started events at X_analytics_id level"
    type:  count_distinct
    sql: ${X_analytics_id};;
    filters: {
      field: event_name
      value: "Checkout Started"
    }
  }

  # Funnel step counted by anonymous_id (Customer identifier).
  measure: checkout_load_customer{
    group_label: "Rudderstack Events and CVR - Customer level"
    description: "Distinct count of Checkout Started events at anonymous_id level"
    type:  count_distinct
    sql: ${anonymous_id};;
    filters: {
      field: event_name
      value: "Checkout Started"
    }
  }
  
  # ... (All subsequent funnel steps, like shipping_info_entered, follow the same structure 
  # of counting distinct event IDs, shopper IDs, customer IDs, etc., based on a filter.) ...
}