package hagan.brian.solr.geoLookupProcessor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.FileReader;
import java.io.IOException;

public class GeoLookupProcessorFactory extends UpdateRequestProcessorFactory {
	//private static final String ZONE_DATA_PATH = "zoneDataPath";
	//String zoneDataPath;

/*	public void init(NamedList args) {
		Logger logger = Logger.getLogger(GeoLookupProcessorFactory.class);
		zoneDataPath = (String)args.remove(ZONE_DATA_PATH);
		logger.info(zoneDataPath);

		super.init(args);
	}*/

	public GeoLookupProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
		return new GeoLookupProcessor(next);
	}
}

class GeoLookupProcessor extends UpdateRequestProcessor {

	GeometryFactory gf;
	WKTReader wktReader;
	FileReader csvReader;
	//FileReader bronxCsvReader;
	Geometry manhattanPolygon;    
	Geometry queensPolygon;
	Geometry bronxPolygon;
	Geometry statenIslandPolygon;
	Geometry brooklynPolygon;
	Logger logger; 

	public GeoLookupProcessor( UpdateRequestProcessor next) {
		super( next );

		gf = new GeometryFactory();
		wktReader = new WKTReader();
		logger = Logger.getLogger(GeoLookupProcessor.class);	
		BasicConfigurator.configure();
		
		try {
			csvReader = new FileReader("/Users/bhagan/Downloads/nybb.csv");	
			//csvReader = new FileReader(zoneDataPath);
			Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(csvReader);
			for (CSVRecord record : records) {
				if(record.get(1).equalsIgnoreCase("Manhattan")) 
					manhattanPolygon = wktReader.read(record.get(2));
				else if(record.get(1).equalsIgnoreCase("Bronx")) 
					bronxPolygon = wktReader.read(record.get(2));
				else if(record.get(1).equalsIgnoreCase("Queens")) 
					queensPolygon = wktReader.read(record.get(2));
				else if(record.get(1).equalsIgnoreCase("Brooklyn")) 
					brooklynPolygon = wktReader.read(record.get(2));
				else if(record.get(1).equalsIgnoreCase("Staten Island")) 
					statenIslandPolygon = wktReader.read(record.get(2));			
			}
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		}
	}


	public boolean addNewFields(SolrInputDocument doc, String pickup_boro_name, String pickup_boro_code, String dropoff_boro_name, String dropoff_boro_code) {

			doc.addField( "pickup_boro_name", pickup_boro_name);
			doc.addField( "pickup_boro_code", pickup_boro_code);
			doc.addField( "dropoff_boro_name", dropoff_boro_name);
			doc.addField( "dropoff_boro_code", dropoff_boro_code);	
			return true;
	}

	public void processAdd(AddUpdateCommand cmd) throws IOException {

		SolrInputDocument doc = cmd.getSolrInputDocument();

		double pickupLongitude = Double.parseDouble(doc.getFieldValue("pickup_longitude").toString());
		double pickupLatitude = Double.parseDouble(doc.getFieldValue("pickup_latitude").toString());
		double dropoffLongitude = Double.parseDouble(doc.getFieldValue("dropoff_longitude").toString());
		double dropoffLatitude = Double.parseDouble(doc.getFieldValue("dropoff_latitude").toString());

		Coordinate pickup = new Coordinate(pickupLongitude, pickupLatitude);
		Coordinate dropoff = new Coordinate(dropoffLongitude, dropoffLatitude);

		Point pickup_locationPoint = gf.createPoint(pickup);
		Point dropoff_locationPoint = gf.createPoint(dropoff);	

		if(manhattanPolygon.contains(pickup_locationPoint)) {			
			if(manhattanPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Manhattan", "1", "Manhattan", "1");
			}
			else if(queensPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Manhattan", "1", "Queens",  "4");
			}
			else if(bronxPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Manhattan", "1", "Bronx",  "2");
			}
			else if(brooklynPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Manhattan", "1", "Brooklyn",  "3");
			}
			else if(statenIslandPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Manhattan", "1", "Staten Island",  "5");
			}
			else {
				logger.debug("No dropoff location");
			}
		}
		else if(queensPolygon.contains(pickup_locationPoint)) {
			if(queensPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Queens", "4", "Queens",  "4");
			}
			else if(manhattanPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Queens", "4", "Manhattan",  "1");	
			}
			else if(bronxPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Queens", "4", "Bronx",  "2");		
			}
			else if(brooklynPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Queens", "4", "Brooklyn",  "3");			
			}
			else if(statenIslandPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Queens", "4", "Staten Island",  "5");				
			}
			else {
				logger.debug("No dropoff location");
			}					
		}
		else if(bronxPolygon.contains(pickup_locationPoint)){
			if(bronxPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Bronx", "4", "Bronx",  "2");				
			}
			else if(manhattanPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Bronx", "4", "Manhattan",  "1");
			}
			else if(queensPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Bronx", "4", "Queens",  "4");				
			}
			else if(brooklynPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Bronx", "4", "Brooklyn",  "3");				
			}
			else if(statenIslandPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Bronx", "4", "Staten Island",  "5");				
			}
			else {
				logger.debug("No dropoff location");
			}		
		}
		else if(brooklynPolygon.contains(pickup_locationPoint)) {
			if(brooklynPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Brooklyn", "3", "Brooklyn",  "3");				
			}
			else if(manhattanPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Brooklyn", "3", "Manhattan",  "1");				
			}
			else if(bronxPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Brooklyn", "3", "Bronx",  "2");				
			}
			else if(queensPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Brooklyn", "3", "Queens",  "5");				
			}
			else if(statenIslandPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Brooklyn", "3", "Staten Island",  "5");				
			}
			else {
				logger.debug("No dropoff location");
			}		
		}
		else if(statenIslandPolygon.contains(pickup_locationPoint)) {
			if(statenIslandPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Staten Island", "5", "Staten Island",  "5");				
			}
			else if(manhattanPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Staten Island", "5", "Manhattan",  "1");						
			}
			else if(bronxPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Staten Island", "5", "Bronx",  "2");							
			}
			else if(queensPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Staten Island", "5", "Queens",  "4");								
			}
			else if(brooklynPolygon.contains(dropoff_locationPoint)) {
				addNewFields(doc, "Staten Island", "5", "Brooklyn",  "3");									
			}
			else {
				logger.debug("No dropoff location");
			}					
		}
		else {
			logger.debug("No pickup location");
		}   
		// pass it up the chain
		super.processAdd(cmd);
	}
}