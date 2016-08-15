/* (c) 2014 - 2015 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.vfny.geoserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.ProjectionPolicy;
import org.geoserver.catalog.ResourcePool;
import org.geoserver.data.test.MockData;
import org.geoserver.data.test.SystemTestData;
import org.geoserver.data.test.SystemTestData.LayerProperty;
import org.geoserver.test.GeoServerSystemTestSupport;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.data.DataAccess;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.GeoTools;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.vfny.geoserver.global.GeoServerFeatureStore;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

public class ProjectionPolicyTest extends GeoServerSystemTestSupport {

    static WKTReader WKT = new WKTReader();
    
    
    @Override
    protected void setUpTestData(SystemTestData testData) throws Exception {
        Map<LayerProperty, Object> props = new HashMap<LayerProperty, Object>();
        props.put(LayerProperty.PROJECTION_POLICY, ProjectionPolicy.FORCE_DECLARED);
        props.put(LayerProperty.DECLARED_SRS, 4269);
        testData.setUpVectorLayer(SystemTestData.BASIC_POLYGONS, props);
        
        props.put(LayerProperty.PROJECTION_POLICY, ProjectionPolicy.REPROJECT_TO_DECLARED);
        props.put(LayerProperty.DECLARED_SRS, 4326);
        props.put(LayerProperty.NATIVE_SRS, 32615);
        testData.setUpVectorLayer(MockData.POLYGONS, props);
        
        props.put(LayerProperty.PROJECTION_POLICY, ProjectionPolicy.NONE);
        props.put(LayerProperty.DECLARED_SRS, 3004);
        testData.setUpVectorLayer(MockData.LINES, props);
        
        props.put(LayerProperty.PROJECTION_POLICY, ProjectionPolicy.REPROJECT_TO_DECLARED);
        props.put(LayerProperty.DECLARED_SRS, 4326);
        props.put(LayerProperty.NAME, "MyPoints");
        testData.setUpVectorLayer(MockData.POINTS, props);
        
        testData.setUpDefaultRasterLayers();
        testData.setUpWcs10RasterLayers();
        testData.setUpSecurity();
    }
    
    @Test
    public void testGetFeatures_Force() throws Exception {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(MockData.BASIC_POLYGONS.getLocalPart());
        assertEquals("EPSG:4269", fti.getSRS());
        assertEquals(ProjectionPolicy.FORCE_DECLARED, fti.getProjectionPolicy());
        FeatureCollection fc = fti.getFeatureSource(null, null).getFeatures();
        assertEquals(CRS.decode("EPSG:4269"), fc.getSchema().getCoordinateReferenceSystem());
        FeatureIterator fi = fc.features();
        Feature f = fi.next();
        fi.close();
        assertEquals(CRS.decode("EPSG:4269"), f.getType().getCoordinateReferenceSystem());
    }
    
    @Test
    public void testGetFeatures_Reproject() throws Exception {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(MockData.POLYGONS.getLocalPart());
        assertEquals("EPSG:4326", fti.getSRS());
        assertEquals((Integer)32615, CRS.lookupEpsgCode(fti.getNativeCRS(), false));
        
        assertEquals(ProjectionPolicy.REPROJECT_TO_DECLARED, fti.getProjectionPolicy());
        FeatureSource<? extends FeatureType, ? extends Feature> featureSource = fti.getFeatureSource(null, null);
                
		FeatureCollection fc = featureSource.getFeatures();
        assertEquals(CRS.decode("EPSG:4326"), fc.getSchema().getCoordinateReferenceSystem());
        FeatureIterator fi = fc.features();
        Feature f = fi.next();
        
        //test that geometry was actually reprojected
        Geometry g = (Geometry) f.getDefaultGeometryProperty().getValue();
        assertFalse(g.equalsExact(WKT.read(
                "POLYGON((500225 500025,500225 500075,500275 500050,500275 500025,500225 500025))")));
        fi.close();
        assertEquals(CRS.decode("EPSG:4326"), f.getType().getCoordinateReferenceSystem());
    }

    @Test
    public void testModify_ReprojectsBackToNative() throws Exception {
    	
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(MockData.POLYGONS.getLocalPart());
        assertEquals("EPSG:4326", fti.getSRS());
        assertEquals((Integer)32615, CRS.lookupEpsgCode(fti.getNativeCRS(), false));        
        assertEquals(ProjectionPolicy.REPROJECT_TO_DECLARED, fti.getProjectionPolicy());
                        
        Geometry newGeometry4326 = WKT.read("POLYGON ((-92.997971 4.523788, -92.997971 4.524241, -92.997520 4.524014, -92.997520 4.523788, -92.997971 4.523788))");
        Geometry expectedReprojected = projectTo32615(newGeometry4326);
        
        // act
        GeoServerFeatureStore featureStore = (GeoServerFeatureStore) fti.getFeatureSource(null, null);
        GeometryDescriptor geometryDescriptor = fti.getFeatureType().getGeometryDescriptor();
        featureStore.modifyFeatures(geometryDescriptor, newGeometry4326, Filter.INCLUDE);
        
        //assert that geometry was actually reprojected to the underlying store
        DataStore dataStore = featureStore.getDataStore();
        SimpleFeatureSource nativeFeatureSource = dataStore.getFeatureSource(MockData.POLYGONS.getLocalPart());
        SimpleFeatureCollection nativeFeatures = nativeFeatureSource.getFeatures(Filter.INCLUDE);
        SimpleFeatureIterator fi = nativeFeatures.features();
        SimpleFeature f = fi.next();
        Geometry g = (Geometry) f.getDefaultGeometryProperty().getValue();
        assertTrue("Modify operation should reproject geometry to native srs", g.equalsExact(expectedReprojected));
        assertEquals(CRS.decode("EPSG:32615"), f.getType().getCoordinateReferenceSystem());
        fi.close();
    }

	private Geometry projectTo32615(Geometry newGeometry4326)
			throws NoSuchAuthorityCodeException, FactoryException,
			TransformException {
		GeometryCoordinateSequenceTransformer transformer = new GeometryCoordinateSequenceTransformer();
        CoordinateReferenceSystem epsg4326 = CRS.decode("EPSG:4326");
		CoordinateReferenceSystem epsg32615 = CRS.decode("EPSG:32615");        
        MathTransform tx = CRS.findMathTransform(epsg4326, epsg32615);
        transformer.setCoordinateReferenceSystem(epsg32615);
        transformer.setMathTransform(tx);
        Geometry expectedReprojected = transformer.transform(newGeometry4326);
		return expectedReprojected;
	}

    
    @Test
    public void testGetFeatures_LeaveNative() throws Exception {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(MockData.LINES.getLocalPart());
        assertEquals("EPSG:3004", fti.getSRS());
        assertEquals(ProjectionPolicy.NONE, fti.getProjectionPolicy());
        FeatureCollection fc = fti.getFeatureSource(null, null).getFeatures();
        assertEquals(CRS.decode("EPSG:32615"), fc.getSchema().getCoordinateReferenceSystem());
        FeatureIterator fi = fc.features();
        Feature f = fi.next();
        
        //test that the geometry was left in tact
        Geometry g = (Geometry) f.getDefaultGeometryProperty().getValue();
        assertTrue(g.equalsExact(WKT.read("LINESTRING(500125 500025,500175 500075)")));
        
        fi.close();
        assertEquals(CRS.decode("EPSG:32615"), f.getType().getCoordinateReferenceSystem());
    }
    
    @Test
    public void testWithRename() throws Exception {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName("MyPoints");
        assertEquals("EPSG:4326", fti.getSRS());
        assertEquals(ProjectionPolicy.REPROJECT_TO_DECLARED, fti.getProjectionPolicy());
        FeatureCollection fc = fti.getFeatureSource(null, null).getFeatures();
        assertEquals(CRS.decode("EPSG:4326"), fc.getSchema().getCoordinateReferenceSystem());
        FeatureIterator fi = fc.features();
        Feature f = fi.next();
        
        //test that geometry was reprojected
        Geometry g = (Geometry) f.getDefaultGeometryProperty().getValue();
        assertFalse(g.equalsExact(WKT.read("POINT(500050 500050)")));
        fi.close();
        assertEquals(CRS.decode("EPSG:4326"), f.getType().getCoordinateReferenceSystem());
    }
    
    @Test
    public void testCoverage_Force() throws Exception {
        // force the data to another projection
        Catalog catalog = getCatalog();
        CoverageInfo ci = catalog.getCoverageByName("usa");
        ci.setProjectionPolicy(ProjectionPolicy.FORCE_DECLARED);
        ci.setSRS("EPSG:3857");
        catalog.save(ci);
        
        ci = catalog.getCoverageByName("usa");
        assertEquals(ProjectionPolicy.FORCE_DECLARED, ci.getProjectionPolicy());
        assertEquals("EPSG:3857", ci.getSRS());
        
        // now get the reader via the coverage info
        GridCoverage2DReader r;
        r = (GridCoverage2DReader) ci.getGridCoverageReader(null, GeoTools.getDefaultHints());
        assertTrue(CRS.equalsIgnoreMetadata(CRS.decode("EPSG:3857"), r.getCoordinateReferenceSystem()));
        
        // and again without any hint
        r = (GridCoverage2DReader) ci.getGridCoverageReader(null, null);
        assertTrue(CRS.equalsIgnoreMetadata(CRS.decode("EPSG:3857"), r.getCoordinateReferenceSystem()));
        
        // get the reader straight: we should get back the native projection
        CoverageStoreInfo store = catalog.getCoverageStoreByName("usa");
        final ResourcePool rpool = catalog.getResourcePool();
        r = (GridCoverage2DReader) rpool.getGridCoverageReader(store, GeoTools.getDefaultHints());
        assertTrue(CRS.equalsIgnoreMetadata(CRS.decode("EPSG:4326"), r.getCoordinateReferenceSystem()));

   }
}
