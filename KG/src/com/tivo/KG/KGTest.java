/**
 * 
 */
package com.tivo.KG;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.tivo.automation.dbutil.DBModule;
import com.tivo.automation.input.InputDetails;
import com.tivo.automation.input.ReadRemoteFile;
import com.tivo.automation.input.ReadSpecificFields;
import com.tivo.automation.input.TestCase;
import com.tivo.automation.jsonutil.AttributeType;
import com.tivo.automation.jsonutil.JsonComparator;
import com.tivo.automation.jsonutil.MappingDetails;

import jxl.read.biff.BiffException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class KGTest {

	
	@Test
	public void test() throws IOException, BiffException, ClassNotFoundException, SQLException, JSONException, org.apache.wink.json4j.JSONException {
		DBModule obj=new DBModule();
		//String suite_names[] = {"GENRE DB", "LANGUAGE DB", "DECADE DB"};
		String suite_names[] = {"MOVIES KSQ"};
		String group_list[] = {"GENRES", "LANGUAGES", "DECADES"};
		Map<String, Map> testcase_dict = new HashMap<String, Map>();
		Map<String, String> file_list = new HashMap<String, String>();
		Map<String, String> fields_dict = new HashMap<String, String>();

		file_list.put("GUIDMERGE", "/home/veveo/datagen/current/seed_data/guid_merge.list");
		file_list.put("GENRE", "/home/veveo/datagen/current/misc_kg_data/genre.data");
		file_list.put("LANGUAGE", "/home/veveo/datagen/current/misc_kg_data/language.data");
		file_list.put("DECADE", "/home/veveo/datagen/current/misc_kg_data/decade.data");
		file_list.put("MOVIES", "/home/veveo/datagen/current/seed_data/movie.data");

		for (int i=0; i < suite_names.length; i++)
		{
			String suite_name= suite_names[i];
			String suite[] = suite_name.split(" ");
			String title = suite[0];
			String name  = suite[1];

			String test_suite_query = String.format("select id, type, record_schema from test_suites where suite_name = '%s' and title = '%s'", title, name);
			System.out.println(test_suite_query);
			String output = obj.runQuery(test_suite_query);
			JSONObject jsonTestSuiteResult = new JSONObject(output);
			JSONArray jsonTestSuiteArray = jsonTestSuiteResult.getJSONArray("Result");
			jsonTestSuiteResult =(JSONObject)jsonTestSuiteArray.get(0);
			String suite_id = (String)jsonTestSuiteResult.get("id");
			String schema = (String)jsonTestSuiteResult.get("record_schema");
			String testcases = obj.runQuery(String.format("select record from test_cases where suite_id='%s'", suite_id));
			InputDetails inputDetails=new InputDetails();
			inputDetails.setTestCaseContent(testcases);
			inputDetails.setSuiteName(title);
			inputDetails.setTitle(name);
			inputDetails.setSchemaDelimiter("#<>#");
			TestCase testcase=new TestCase();
			JSONObject testCaseJson=testcase.parseTestCase(inputDetails);
			
			ReadSpecificFields spcFields = new ReadSpecificFields();
			System.out.println("Reading file");
			
				InputStream stream =new FileInputStream(new File("/Users/skaliyaperumal/movie.data"));
                LineNumberReader reader  = new LineNumberReader(new InputStreamReader(stream));
           
            String lineRead = "";
            StringBuilder op=new StringBuilder();
            while ((lineRead = reader.readLine()) != null) {
                            op.append(lineRead);
                            op.append("\n");
           
                           
          
				}

				String file=op.toString();
				System.out.println(file.length());
				//String fileContent = readRemoteFiles.readInput(inputDetails);
				inputDetails.setFileContent(file);
				String field_names[] = schema.split("#<>#");
				String attrs[] = field_names;
				inputDetails.setRequiredFields(attrs);
				JSONObject fieldData = spcFields.readInput(inputDetails);
				//System.out.println(fieldData);
				//JSONObject jsonResult = new JSONObject(fieldData);
				JSONArray jsonResultArray = fieldData.getJSONArray("Result");
				System.out.println(jsonResultArray.length());
				MappingDetails mappingDetails = new MappingDetails();
				mappingDetails.setPrimaryAttribute("Gi");
				mappingDetails.setTestingAttribute("Gg");
				mappingDetails.setPartialCheck(true);
				mappingDetails.setMergeRequired(true);
				mappingDetails.setPrimaryAttributeType(AttributeType.String);
				
				//
				Map<String, String> resultMap=new HashMap();
				ReadRemoteFile readRemoteFile=new ReadRemoteFile();
				  inputDetails.setSshHost("10.28.218.80");
				    inputDetails.setSshLoginId("veveo");
				    inputDetails.setSshPassword("veveo123");
				 inputDetails.setSshFilePath("/home/veveo/datagen/current/seed_data/guid_merge.list");
			        JSONObject mergeDataJson=readRemoteFile.readInput(inputDetails);
			        String mergeDataString=mergeDataJson.getString("Result");
			        String[] mergeData=mergeDataString.split("\n");
			        for(String data:mergeData) {
			        //	System.out.println(data);
			        	String[] keyValue=data.split("\\<\\>");
			        	resultMap.put(keyValue[0], keyValue[1]);
			        }
				//
			        System.out.println("map size"+resultMap.size());
			        mappingDetails.setMapMerge(resultMap); 
			        //
					Map<String, String> resultMapReverse=new HashMap();
					// inputDetails.setSshFilePath("/home/veveo/datagen/current/seed_data/guid_merge.list");
				      
				        for(String data:mergeData) {
				        //	System.out.println(data);
				        	String[] keyValue=data.split("\\<\\>");
				        	resultMapReverse.put(keyValue[1], keyValue[0]);
				        }
					//
				        System.out.println("map size"+resultMapReverse.size());
				        mappingDetails.setMapMergeReverse(resultMapReverse); 
				JsonComparator jsonComparator = new JsonComparator();
				//System.out.println(testCaseJson);
				//System.out.println(fieldData);
				String result=jsonComparator.compareJson(testCaseJson, fieldData, mappingDetails);
				//System.out.println(result);

				if(result.contains("false")) {
					Assert.assertEquals(true, false,result);

				}
				else {
					Assert.assertEquals(true, true);
				}



			
		}
		
	}

}
