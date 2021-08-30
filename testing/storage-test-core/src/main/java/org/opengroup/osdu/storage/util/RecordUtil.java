// Copyright 2017-2019, Schlumberger
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.opengroup.osdu.storage.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.opengroup.osdu.core.common.Constants;

public class RecordUtil {

    public static String createDefaultJsonRecord(String id, String kind, String legalTag) {
        JsonObject record = getDefaultRecordWithDefaultData(id, kind, legalTag);
        JsonArray records = new JsonArray();
        records.add(record);
        return records.toString();
    }

	public static String createDefaultJsonRecordWithParentId(String id, String kind, String legalTag, String parentId) {
		JsonObject record = getDefaultRecordWithDefaultData(id, kind, legalTag);
		JsonObject ancestryObject = new JsonObject();
		JsonArray parents = new JsonArray();
		parents.add(parentId);
		ancestryObject.add("parents", parents);
		record.add("ancestry", ancestryObject);
		JsonArray records = new JsonArray();
		records.add(record);
		return records.toString();
	}

    public static String createDefaultJsonRecords(int recordsCount, String id, String kind, String legalTag) {
        JsonArray records = new JsonArray();
        for (int i = 0; i < recordsCount; i++) {
            JsonObject record = getDefaultRecordWithDefaultData(id +i, kind, legalTag);
            records.add(record);
        }
        return records.toString();
    }

	public static String createJsonRecordWithData(String id, String kind, String legalTag, String data) {

		JsonObject dataJson = new JsonObject();
		dataJson.addProperty("custom", data);
		dataJson.addProperty("score-int", 58377304471659395L);
		dataJson.addProperty("score-double", 58377304.471659395);

		JsonObject record = getRecordWithInputData(id, kind, legalTag, dataJson);

		JsonArray records = new JsonArray();
		records.add(record);

		return records.toString();
	}

	public static String createJsonRecordWithEntV2OnlyAcl(String id, String kind, String legalTag, String data) {
		JsonObject dataJson = new JsonObject();
		dataJson.addProperty("custom", data);
		dataJson.addProperty("score-int", 58377304471659395L);
		dataJson.addProperty("score-double", 58377304.471659395);

		JsonObject record = getRecordWithInputDataAndAcl(id, kind, legalTag, dataJson);

		JsonArray records = new JsonArray();
		records.add(record);

		return records.toString();

	}

	public static String createJsonRecordWithReference(int recordsCount, String id, String kind, String legalTag, String fromCrs, String conversionType) {

		JsonArray records = new JsonArray();

		for (int i = 0; i < recordsCount; i++) {

			JsonObject data = new JsonObject();
			data.addProperty("X", 16.00);
			data.addProperty("Y", 10.00);
			data.addProperty("Z", 0.0);

			JsonArray propertyNames = new JsonArray();
			propertyNames.add("X");
			propertyNames.add("Y");
			propertyNames.add("Z");

			JsonObject meta = new JsonObject();
			meta.addProperty(Constants.KIND, conversionType);
			meta.addProperty(Constants.PERSISTABLE_REFERENCE, fromCrs);
			meta.add(Constants.PROPERTY_NAMES, propertyNames);

			JsonArray metaBlocks = new JsonArray();
			metaBlocks.add(meta);

			JsonObject record = getRecordWithInputData(id + i, kind, legalTag, data);
			record.add(Constants.META, metaBlocks);

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordMissingValue(int recordsCount, String id, String kind, String legalTag, String fromCrs, String conversionType) {

		JsonArray records = new JsonArray();

		for (int i = 0; i < recordsCount; i++) {

			JsonObject data = new JsonObject();
			data.addProperty("X", 16.00);
			data.addProperty("Z", 0.0);

			JsonArray propertyNames = new JsonArray();
			propertyNames.add("X");
			propertyNames.add("Y");
			propertyNames.add("Z");

			JsonObject meta = new JsonObject();
			meta.addProperty(Constants.KIND, conversionType);
			meta.addProperty(Constants.PERSISTABLE_REFERENCE, fromCrs);
			meta.add(Constants.PROPERTY_NAMES, propertyNames);

			JsonArray metaBlocks = new JsonArray();
			metaBlocks.add(meta);

			JsonObject record = getRecordWithInputData(id + i, kind, legalTag, data);
			record.add(Constants.META, metaBlocks);

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordNoMetaBlock(int recordsCount, String id, String kind, String legalTag) {

		JsonArray records = new JsonArray();

		for (int i = 0; i < recordsCount; i++) {
			JsonObject data = new JsonObject();
			data.addProperty("X", 16.00);
			data.addProperty("Y", 16.00);
			data.addProperty("Z", 0.0);

			JsonObject record = getRecordWithInputData(id + i, kind, legalTag, data);
			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordsWithDateFormat(int recordsCount, String id, String kind, String legalTag, String format, String propertyName, String date, String persistableReference) {

		JsonArray records = new JsonArray();

		for (int i = 0; i < recordsCount; i++) {
			JsonObject data = new JsonObject();
			data.addProperty(propertyName, date);

			JsonArray propertyNames = new JsonArray();
			propertyNames.add(propertyName);

			JsonObject meta = new JsonObject();
			meta.addProperty(Constants.PERSISTABLE_REFERENCE, persistableReference);
			meta.addProperty(Constants.KIND, "DateTime");
			meta.add(Constants.PROPERTY_NAMES, propertyNames);

			JsonArray metas = new JsonArray();
			metas.add(meta);

			JsonObject record = getRecordWithInputData(id + i, kind, legalTag, data);
			record.add(Constants.META, metas);
			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordWithNestedProperty(int recordsNumber, String id, String kind, String legalTag, String fromCrs, String conversionType) {

		JsonArray records = new JsonArray();

		for (int i = 0; i < 8 + recordsNumber; i++) {

			JsonArray pointValues1 = new JsonArray();
			pointValues1.add(16.00);
			pointValues1.add(10.00);
			JsonArray pointValues2 = new JsonArray();
			pointValues2.add(16.00);
			pointValues2.add(10.00);
			JsonArray points = new JsonArray();
			points.add(pointValues1);
			points.add(pointValues2);

			JsonObject nestedProperty = new JsonObject();
			nestedProperty.addProperty("crsKey", "Native");
			nestedProperty.add(Constants.POINTS, points);

			JsonObject data = new JsonObject();
			data.addProperty("message", "integration-test-record");
			data.add("projectOutlineLocalGeographic", nestedProperty);

			JsonArray propertyNames = new JsonArray();
			propertyNames.add("projectOutlineLocalGeographic");

			JsonObject meta = new JsonObject();
			meta.addProperty(Constants.KIND, conversionType);
			meta.addProperty(Constants.PERSISTABLE_REFERENCE, fromCrs);
			meta.add(Constants.PROPERTY_NAMES, propertyNames);

			JsonArray metaBlocks = new JsonArray();
			metaBlocks.add(meta);

			JsonObject record = getRecordWithInputData(id + i, kind, legalTag, data);
			record.add(Constants.META, metaBlocks);

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordWithNestedArrayOfProperties(int recordsNumber, String id, String kind, String legalTag, String fromRef, String conversionType) {
		JsonArray records = new JsonArray();

		for (int i = 12; i < 12 + recordsNumber; i++) {

			JsonArray nestedArray = new JsonArray();
			JsonObject item1 = new JsonObject();
			item1.addProperty("measuredDepth", 10.0);
			item1.addProperty("otherField", "testValue1");
			JsonObject item2 = new JsonObject();
			item2.addProperty("measuredDepth", 20.0);
			item2.addProperty("otherField", "testValue2");
			nestedArray.add(item1);
			nestedArray.add(item2);

			JsonObject record = createJsonObjectRecordWithNestedArray(nestedArray, id + i, kind, legalTag, conversionType, fromRef, "markers[].measuredDepth");

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordWithNestedArrayOfPropertiesAndInvalidValues(int recordsNumber, String id, String kind, String legalTag, String fromRef, String conversionType) {
		JsonArray records = new JsonArray();

		for (int i = 12; i < 12 + recordsNumber; i++) {

			JsonArray nestedArray = new JsonArray();
			JsonObject item1 = new JsonObject();
			item1.addProperty("measuredDepth", 10.0);
			item1.addProperty("otherField", "testValue1");
			JsonObject item2 = new JsonObject();
			item2.addProperty("measuredDepth", "invalidValue");
			item2.addProperty("otherField", "testValue2");
			nestedArray.add(item1);
			nestedArray.add(item2);

			JsonObject record = createJsonObjectRecordWithNestedArray(nestedArray, id + i, kind, legalTag, conversionType, fromRef, "markers[].measuredDepth");

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordWithInhomogeneousNestedArrayOfProperties(int recordsNumber, String id, String kind, String legalTag, String fromRef, String conversionType) {
		JsonArray records = new JsonArray();

		for (int i = 13; i < 13 + recordsNumber; i++) {

			JsonArray nestedArray = new JsonArray();
			JsonObject item1 = new JsonObject();
			item1.addProperty("measuredDepth", 10.0);
			item1.addProperty("otherField", "testValue1");
			JsonObject item2 = new JsonObject();
			item2.addProperty("measuredDepth", 20.0);
			item2.addProperty("otherField", "testValue2");
			nestedArray.add(item1);
			nestedArray.add(item2);

			JsonObject record = createJsonObjectRecordWithNestedArray(nestedArray, id + i, kind, legalTag, conversionType, fromRef, "markers[1].measuredDepth");

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordWithInhomogeneousNestedArrayOfPropertiesAndInvalidValues(int recordsNumber, String id, String kind, String legalTag, String fromRef, String conversionType) {
		JsonArray records = new JsonArray();

		for (int i = 13; i < 13 + recordsNumber; i++) {

			JsonArray nestedArray = new JsonArray();
			JsonObject item1 = new JsonObject();
			item1.addProperty("measuredDepth", 10.0);
			item1.addProperty("otherField", "testValue1");
			JsonObject item2 = new JsonObject();
			item2.addProperty("measuredDepth", "invalidValue");
			item2.addProperty("otherField", "testValue2");
			nestedArray.add(item1);
			nestedArray.add(item2);

			JsonObject record = createJsonObjectRecordWithNestedArray(nestedArray, id + i, kind, legalTag, conversionType, fromRef, "markers[1].measuredDepth");

			records.add(record);
		}

		return records.toString();
	}

	public static String createJsonRecordWithInhomogeneousNestedArrayOfPropertiesAndIndexOutOfBoundary(int recordsNumber, String id, String kind, String legalTag, String fromRef, String conversionType) {
		JsonArray records = new JsonArray();

		for (int i = 13; i < 13 + recordsNumber; i++) {

			JsonArray nestedArray = new JsonArray();
			JsonObject item1 = new JsonObject();
			item1.addProperty("measuredDepth", 10.0);
			item1.addProperty("otherField", "testValue1");
			JsonObject item2 = new JsonObject();
			item2.addProperty("measuredDepth", "20.0");
			item2.addProperty("otherField", "testValue2");
			nestedArray.add(item1);
			nestedArray.add(item2);

			JsonObject record = createJsonObjectRecordWithNestedArray(nestedArray, id + i, kind, legalTag, conversionType, fromRef, "markers[2].measuredDepth");

			records.add(record);
		}

		return records.toString();
	}

	private static JsonObject createJsonObjectRecordWithNestedArray(JsonArray nestedArray, String id, String kind, String legalTag, String conversionType, String fromRef, String propertyName) {
		JsonObject data = new JsonObject();
		data.addProperty("message", "integration-test-record");
		data.add("markers", nestedArray);

		JsonArray propertyNames = new JsonArray();
		propertyNames.add(propertyName);

		JsonObject meta = new JsonObject();
		meta.addProperty(Constants.KIND, conversionType);
		meta.addProperty(Constants.PERSISTABLE_REFERENCE, fromRef);
		meta.add(Constants.PROPERTY_NAMES, propertyNames);

		JsonArray metaBlocks = new JsonArray();
		metaBlocks.add(meta);

		JsonObject record = getRecordWithInputData(id, kind, legalTag, data);
		record.add(Constants.META, metaBlocks);

		return record;
	}

	private static JsonObject createJsonObjectRecordWithNestedArray(JsonArray nestedArray, String id, String kind, String legalTag, String conversionType, String fromRef) {
		JsonObject data = new JsonObject();
		data.addProperty("message", "integration-test-record");
		data.add("markers", nestedArray);

		JsonArray propertyNames = new JsonArray();
		propertyNames.add("markers[].measuredDepth");

		JsonObject meta = new JsonObject();
		meta.addProperty(Constants.KIND, conversionType);
		meta.addProperty(Constants.PERSISTABLE_REFERENCE, fromRef);
		meta.add(Constants.PROPERTY_NAMES, propertyNames);

		JsonArray metaBlocks = new JsonArray();
		metaBlocks.add(meta);

		JsonObject record = getRecordWithInputData(id, kind, legalTag, data);
		record.add(Constants.META, metaBlocks);

		return record;
	}

	public static String createJsonRecordWithMultiplePairOfCoordinates(int recordsNumber, String id, String kind, String legalTag, String fromCrs, String conversionType) {
		JsonArray records = new JsonArray();
		for (int i = 0; i <  recordsNumber; i++) {

			JsonObject data = new JsonObject();
			data.addProperty("X", 16.00);
			data.addProperty("Y", 10.00);
			data.addProperty("LON", 16.00);
			data.addProperty("LAT", 10.00);

			JsonArray propertyNames = new JsonArray();
			propertyNames.add("X");
			propertyNames.add("Y");
			propertyNames.add("LON");
			propertyNames.add("LAT");

			JsonObject meta = new JsonObject();
			meta.addProperty(Constants.KIND, conversionType);
			meta.addProperty(Constants.PERSISTABLE_REFERENCE, fromCrs);
			meta.add(Constants.PROPERTY_NAMES, propertyNames);

			JsonArray metaBlocks = new JsonArray();
			metaBlocks.add(meta);

			JsonObject record = getDefaultRecord(id + i, kind, legalTag);
			record.add(Constants.DATA, data);
			record.add(Constants.META, metaBlocks);

			records.add(record);
		}
		return records.toString();
	}

	public static String createJsonRecordWithAsIngestedCoordinates(int recordsNumber, String id, String kind, String legalTag, String prCRS, String prUNITZ, String geometryType, String attributeType) {
		JsonArray records = new JsonArray();
		Gson gson = new Gson();
		for (int i = 0; i <  recordsNumber; i++) {
			JsonObject data = new JsonObject();

			JsonObject properties = new JsonObject();
			JsonObject geometry = new JsonObject();

			switch (geometryType) {
				case Constants.ANY_CRS_POINT:
					geometry.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates1(1,2)));
					break;
				case Constants.ANY_CRS_MULTIPOINT:
				case Constants.ANY_CRS_LINE_STRING:
					geometry.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates2(1,2)));
					break;
				case Constants.ANY_CRS_MULTILINE_STRING:
				case Constants.ANY_CRS_POLYGON:
					geometry.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates3(1,2)));
					break;
				case Constants.ANY_CRS_MULTIPOLYGON:
					geometry.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates4(1,2)));
					break;
				case Constants.ANY_CRS_GEOMETRY_COLLECTION:
					JsonArray geometries = new JsonArray();
					JsonObject geometriesObj = new JsonObject();
					geometriesObj.addProperty(Constants.BBOX, (Boolean) null);
					geometriesObj.addProperty(Constants.TYPE, Constants.POINT);
					geometriesObj.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates1(1,2)));
					geometries.add(geometriesObj);
					geometry.add(Constants.GEOMETRIES, geometries);
					break;
			}
			geometry.addProperty(Constants.TYPE, geometryType);
			geometry.addProperty(Constants.BBOX, (Boolean) null);

			JsonArray features = new JsonArray();
			JsonObject feature = new JsonObject();
			feature.addProperty(Constants.BBOX, (Boolean) null);
			feature.addProperty(Constants.TYPE, Constants.ANY_CRS_FEATURE);
			feature.add(Constants.PROPERTIES, properties);
			feature.add(Constants.GEOMETRY, geometry);
			features.add(feature);

			JsonObject asIngestedCoordinates = new JsonObject();
			asIngestedCoordinates.addProperty(Constants.PERSISTABLE_REFERENCE_CRS, prCRS);
			asIngestedCoordinates.addProperty(Constants.PERSISTABLE_REFERENCE_UNIT_Z, prUNITZ);
			asIngestedCoordinates.addProperty(Constants.TYPE, Constants.ANY_CRS_FEATURE_COLLECTION);
			asIngestedCoordinates.add(Constants.PROPERTIES, properties);
			asIngestedCoordinates.add(Constants.FEATURES, features);

			JsonObject validAttribute = new JsonObject();
			validAttribute.add(Constants.AS_INGESTED_COORDINATES, asIngestedCoordinates);

			data.add(attributeType, validAttribute);

			JsonObject record = getDefaultRecord(id + i, kind, legalTag);
			record.add(Constants.DATA, data);
			records.add(record);
		}
		return records.toString();
	}

	public static String createJsonRecordWithInvalidAsIngestedCoordinates(int recordsNumber, String id, String kind, String legalTag, String prCRS, String prUNITZ, String geometryType, String attributeType) {
		JsonArray records = new JsonArray();
		Gson gson = new Gson();
		for (int i = 0; i <  recordsNumber; i++) {
			JsonObject data = new JsonObject();
			JsonObject geometry = new JsonObject();
			geometry.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates1(1,2)));
			geometry.addProperty(Constants.TYPE, geometryType);
			geometry.addProperty(Constants.BBOX, (Boolean) null);

			JsonArray features = new JsonArray();
			JsonObject feature = new JsonObject();
			feature.addProperty(Constants.BBOX, (Boolean) null);
			feature.addProperty(Constants.TYPE, Constants.ANY_CRS_FEATURE);
			feature.add(Constants.GEOMETRY, geometry);
			features.add(feature);

			JsonObject asIngestedCoordinates = new JsonObject();
			asIngestedCoordinates.addProperty(Constants.PERSISTABLE_REFERENCE_CRS, prCRS);
			asIngestedCoordinates.addProperty(Constants.PERSISTABLE_REFERENCE_UNIT_Z, prUNITZ);
			asIngestedCoordinates.addProperty(Constants.TYPE, Constants.ANY_CRS_FEATURE_COLLECTION);
			asIngestedCoordinates.add("features1", features);

			JsonObject validAttribute = new JsonObject();
			validAttribute.add(Constants.AS_INGESTED_COORDINATES, asIngestedCoordinates);

			data.add(attributeType, validAttribute);

			JsonObject record = getDefaultRecord(id + i, kind, legalTag);
			record.add(Constants.DATA, data);
			records.add(record);
		}
		return records.toString();
	}

	public static String createJsonRecordWithWGS84Coordinates(int recordsNumber, String id, String kind, String legalTag, String prCRS, String prUNITZ, String geometryType, String attributeType) {
		JsonArray records = new JsonArray();
		Gson gson = new Gson();
		for (int i = 0; i <  recordsNumber; i++) {
			JsonObject data = new JsonObject();

			JsonArray coordinates = new JsonArray();
			coordinates.add(313405.9477893702);
			coordinates.add(6544797.620047403);
			coordinates.add(6.561679790026246);

			JsonObject geometry = new JsonObject();
			geometry.add(Constants.COORDINATES, gson.toJsonTree(createCoordinates1(1,2)));
			geometry.addProperty(Constants.TYPE, geometryType);
			geometry.addProperty(Constants.BBOX, (Boolean) null);

			JsonArray features = new JsonArray();
			JsonObject feature = new JsonObject();
			feature.addProperty(Constants.BBOX, (Boolean) null);
			feature.addProperty(Constants.TYPE, Constants.ANY_CRS_FEATURE);
			feature.add(Constants.GEOMETRY, geometry);
			features.add(feature);

			JsonObject asIngestedCoordinates = new JsonObject();
			asIngestedCoordinates.addProperty(Constants.PERSISTABLE_REFERENCE_CRS, prCRS);
			asIngestedCoordinates.addProperty(Constants.PERSISTABLE_REFERENCE_UNIT_Z, prUNITZ);
			asIngestedCoordinates.addProperty(Constants.TYPE, Constants.ANY_CRS_FEATURE_COLLECTION);
			asIngestedCoordinates.add(Constants.FEATURES, features);

			JsonArray wgsCoordinates = new JsonArray();
			wgsCoordinates.add(5.7500000010406245);
			wgsCoordinates.add(59.000000000399105);
			wgsCoordinates.add(1.9999999999999998);

			JsonObject wgsGeometry = new JsonObject();
			wgsGeometry.add(Constants.COORDINATES, wgsCoordinates);
			wgsGeometry.addProperty(Constants.TYPE, Constants.POINT);
			wgsGeometry.addProperty(Constants.BBOX, (Boolean) null);

			JsonArray wgsFeatures = new JsonArray();
			JsonObject wgsFeature = new JsonObject();
			wgsFeature.addProperty(Constants.BBOX, (Boolean) null);
			wgsFeature.add(Constants.GEOMETRY, wgsGeometry);
			wgsFeatures.add(wgsFeature);

			JsonObject wgs84Coordinates = new JsonObject();
			wgs84Coordinates.addProperty(Constants.PERSISTABLE_REFERENCE_CRS, (Boolean) null);
			wgs84Coordinates.addProperty(Constants.PERSISTABLE_REFERENCE_UNIT_Z, prUNITZ);
			wgs84Coordinates.addProperty(Constants.TYPE, Constants.FEATURE_COLLECTION);
			wgs84Coordinates.add(Constants.FEATURES, wgsFeatures);

			JsonObject validAttribute = new JsonObject();
			validAttribute.add(Constants.AS_INGESTED_COORDINATES, asIngestedCoordinates);
			validAttribute.add(Constants.WGS84_COORDINATES, wgs84Coordinates);

			data.add(attributeType, validAttribute);

			JsonObject record = getDefaultRecord(id + i, kind, legalTag);
			record.add(Constants.DATA, data);
			records.add(record);
		}
		return records.toString();
	}

	private static double[][][][] createCoordinates4(int mode, int dimension) {
		double[][][][] pts_s = new double[2][2][5][dimension];
		for (int l = 0; l < 2; l++) {
			for (int k = 0; k < pts_s[0].length; k++) {
				double[][][] pts = createCoordinates3(mode, dimension);
				for (int j = 0; j < pts[0].length; j++) {
					for (int i = 0; i < dimension; i++) {
						pts_s[l][k][j][i] = pts[k][j][i] + (k + l) * 4;
					}
				}
			}
		}
		return pts_s;
	}

	private static double[] createCoordinates1(int mode, int dimension) {
		double[] pt_ac = new double[]{500000, 6500000, 1000};
		double[] pt_gj = new double[]{3, 60, 2000};
		double[] pts = new double[dimension];
		if (mode == 0) System.arraycopy(pt_gj, 0, pts, 0, dimension);
		else System.arraycopy(pt_ac, 0, pts, 0, dimension);
		return pts;
	}

	private static double[][] createCoordinates2(int mode, int dimension) {
		double[][] s = new double[][]{{-1, 1, 10}, {1, 1, 10}, {1, -1, 20}, {-1, -1, 20}, {-1, 1, 10}};
		double[][] pts = new double[5][dimension];
		double[] pt = createCoordinates1(mode, dimension);
		for (int j = 0; j < 5; j++) {
			for (int i = 0; i < dimension; i++) {
				pts[j][i] = pt[i] + s[j][i];
			}
		}
		return pts;
	}

	private static double[][][] createCoordinates3(int mode, int dimension) {
		double[][][] pts_s = new double[2][5][dimension];
		for (int k = 0; k < pts_s.length; k++) {
			double[][] pts = createCoordinates2(mode, dimension);
			for (int j = 0; j < pts.length; j++) {
				for (int i = 0; i < dimension; i++) {
					pts_s[k][j][i] = pts[j][i] + k * 4;
				}
			}
		}
		return pts_s;
	}

	private static JsonObject getDefaultRecord(String id, String kind, String legalTag) {
		JsonArray acls = new JsonArray();
		acls.add(TestUtils.getAcl());
		return getDefaultRecordFromAcl(id, kind, legalTag, acls);
	}

	private static JsonObject getDefaultRecordWithEntV2OnlyAcl(String id, String kind, String legalTag) {
		JsonArray acls = new JsonArray();
		acls.add(TestUtils.getEntV2OnlyAcl());
		return getDefaultRecordFromAcl(id, kind, legalTag, acls);
	}

	private static JsonObject getDefaultRecordFromAcl(String id, String kind, String legalTag, JsonArray acls) {
		JsonObject acl = new JsonObject();
		acl.add("viewers", acls);
		acl.add("owners", acls);

		JsonArray tags = new JsonArray();
		tags.add(legalTag);

		JsonArray ordcJson = new JsonArray();
		ordcJson.add("BR");

		JsonObject legal = new JsonObject();
		legal.add("legaltags", tags);
		legal.add("otherRelevantDataCountries", ordcJson);

		JsonObject record = new JsonObject();
		record.addProperty("id", id);
		record.addProperty("kind", kind);
		record.add("acl", acl);
		record.add("legal", legal);
		return record;
	}

	private static JsonObject getDefaultRecordWithDefaultData(String id, String kind, String legalTag) {
		JsonObject data = new JsonObject();
		data.add("int-tag", getNumberPropertyObject("score-int", 58377304471659395L));
		data.add("double-tag", getNumberPropertyObject("score-double", 58377304.471659395));
		data.addProperty("count", 123456789L);
		JsonObject record = getRecordWithInputData(id, kind, legalTag, data);
		return record;
	}

	private static JsonObject getRecordWithInputData(String id, String kind, String legalTag, JsonObject data) {
		JsonObject record = getDefaultRecord(id, kind, legalTag);
		record.add("data", data);
		return record;
	}

	private static JsonObject getRecordWithInputDataAndAcl(String id, String kind, String legalTag, JsonObject data) {
		JsonObject record = getDefaultRecordWithEntV2OnlyAcl(id, kind, legalTag);
		record.add("data", data);
		return record;
	}

	private static JsonObject getNumberPropertyObject(String propertyName, Number intValue) {
		JsonObject numberProperty = new JsonObject();
		numberProperty.addProperty(propertyName, intValue);
		return numberProperty;
	}
}