# SolrGeoLookupProcessor

This project is an implementation of Solr's UpdateRequestProcessor.

I designed this processor to index the NYC Taxi data. The NYC Taxi Trip Data provided by the city contains the following fields:

- medallion
- hack_license
- vendor_id
- rate_code
- store_and_fwd_flag
- pickup_datetime
- dropoff_datetime
- passenger_count
- trip_time_in_secs
- trip_distance
- pickup_longitude
- pickup_latitude
- dropoff_longitude
- dropoff_latitude

The data set provides lat/lon, but does not provide boro names. For my analysis, I want to
include boro names and boro codes in the data set.

 This Solr processor performs the following:
  - Initializes by reading a wkt file of boro information
  - Creates Polygon objects for the boros
  - Creates Coordinate and Point objects for each record
  - Checks the Point against each Boro Polygon 
  - Adds BoroCode and BoroName fields to the record for each match
  
I do use Nifi for many types of transformations. In this case, I've decided that pushing
this processing down to Solr is more effective. By deploying this processor to Solr, I've
reduced my Nifi flow down to just two Nifi processors:

- GetFile (source file)
- PutSolrContentStream
