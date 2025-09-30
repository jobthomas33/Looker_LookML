# This include statement pulls in the dimensions and measures defined in the 
# specific view file 'event.view.lkml'. This makes all fields from that view 
# available for querying within this 'events' Explore.
include: "/event.view.lkml"

explore: events {
  # Friendly, user-facing label for the Explore in the Looker menu.
  label: "Event Analytics"

  # Detailed description to inform users what data is contained within this Explore.
  description: "Contains event tracking data collected/provided by Rudderstack.
Information provided in the Explore relates to areas such as the Event, Device and Product.


  # Access Grant: Restricts visibility of this Explore to only users who possess 
  # the 'can_view_event_analytics' user attribute or permission set.

  required_access_grants: [can_view_event_analytics]

  # --- Join to Device Info ---
  join: analytics_event_device_info {
    # Relationship: 'many_to_one' implies that many events (rows) can map to a 
    # single device information record.
    
    relationship: many_to_one
    
    # SQL_ON: This complex composite join key links the tables based on 
    # a combination of anonymous ID, analytics ID, and the date the event was loaded.
    # The IFNULL/CONCAT structure is necessary to ensure the join handles cases 
    # where the anonymous_id or analytics_id might be NULL, creating a consistent 
    # key for time-based session or device data.

    sql_on:  concat(ifnull(${events.anonymous_id},''),ifnull(${events.analytics_id},''),ifnull(date(${events.loaded_raw}),'1900-01-01'))
    =concat(ifnull(${analytics_event_device_info.anonymous_id},''),ifnull(${analytics_event_device_info.analytics_id},''),ifnull(date(${analytics_event_device_info.loaded_raw}),'1900-01-01'));;
  }


  # --- Join to Retailer Brands ---
  join: retailer_brands {
    # SQL_ON: Simple join linking the event's tenant code to the retailer's brand details.
    sql_on: ${retailer_brands.tenant_code}=${events.tenant_code} ;;
    # Type: 'left_outer' ensures all event records are kept, even if a matching 
    # brand record is missing or not yet configured.

    type: left_outer
    # Relationship: 'one_to_one' suggests each tenant_code is uniquely mapped to a brand.

    relationship: one_to_one
  }

  # Always Filter: Enforces a default filter upon loading the Explore.
  # This serves two main purposes: 1) Improving initial query performance by scanning 
  # less data, and 2) focusing the user's initial view on recent activity (7 days).
  
  always_filter: {
    filters: [events._partitiondate_date: "7 days"]
  }

}
