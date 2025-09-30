include: "/Fact_PreOrders/mba_output.view.lkml"

explore: events {
  label: "Event Analytics"
  description: "Contains event tracking data collected/provided by Rudderstack.
Information provided in the Explore relates to areas such as the Event, Device and Product.
Explore Level: Rudderstack Events Base Table: events"
  required_access_grants: [can_view_event_analytics]
  join: analytics_event_device_info {
    relationship: many_to_one
    sql_on:  concat(ifnull(${events.anonymous_id},''),ifnull(${events.analytics_id},''),ifnull(date(${events.loaded_raw}),'1900-01-01'))
    =concat(ifnull(${analytics_event_device_info.anonymous_id},''),ifnull(${analytics_event_device_info.analytics_id},''),ifnull(date(${analytics_event_device_info.loaded_raw}),'1900-01-01'));;
  }


  join: retailer_brands {
    sql_on: ${retailer_brands.tenant_code}=${events.tenant_code} ;;
    type: left_outer
    relationship: one_to_one
  }
  always_filter: {
    filters: [events._partitiondate_date: "7 days"]
  }

}