package com.google.cloud.genomics.cba;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
* APIs for running the CURL requests
*/
	

public class CurlHttpRequests {

	   /**
	   * This method is used to create a process and execute CURL command. 
	   * @param cmdline A command line sent for execution
	   * @param directory The directory that the command is executed.   
	   * @return output An ArrayList<String> which equals to HttpResponse.toString 
	   */
	public static ArrayList<String> execution(final String cmdline, final String directory) {
		try {
			Process process = new ProcessBuilder(new String[] { "bash", "-c", cmdline }).redirectErrorStream(true)
					.directory(new File(directory)).start();

			ArrayList<String> output = new ArrayList<String>();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = null;
			while ((line = br.readLine()) != null)
				output.add(line);

			// There should really be a timeout here.
			if (0 != process.waitFor())
				return null;
			// System.out.println("output: " + output.toString());

			return output;

		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	static String command(String cmdline) {
		ArrayList<String> output = execution(cmdline, ".");
		if (null == output) {
			System.out.println("\n\n\t\tCOMMAND FAILED: " + cmdline);
			System.exit(0);
		} else
			for (String line : output)
				System.out.println(line);

		return output.get(0);
	}
//TO-Do
//	public HttpResponse http(String url, String body) {
//
//		try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
//			HttpPost request = new HttpPost(url);
//			StringEntity params = new StringEntity(body);
//			request.addHeader("content-type", "application/json");
//			request.setEntity(params);
//			HttpResponse result = httpClient.execute(request);
//
//			String json = EntityUtils.toString(result.getEntity(), "UTF-8");
//			com.google.gson.Gson gson = new com.google.gson.Gson();
//			Response respuesta = gson.fromJson(json, Response.class);
//
//			System.out.println(respuesta.getCode());
//			System.out.println(respuesta.getMessage());
//
//		} catch (IOException ex) {
//		}
//		return null;
//	}
//
//	public class Response {
//
//		private int code;
//		private String message;
//
//		public int getCode() {
//			return code;
//		}
//
//		public void setCode(int _code) {
//			this.code = _code;
//		}
//
//		public String getMessage() {
//			return message;
//		}
//
//		public void setMessage(String _message) {
//			this.message = _message;
//		}
//	}

}
