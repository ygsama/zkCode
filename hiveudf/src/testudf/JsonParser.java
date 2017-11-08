package testudf;

import org.apache.hadoop.hive.ql.exec.UDF;

import parquet.org.codehaus.jackson.map.ObjectMapper;

/**
 * 编写自定义hive函数
 * @author G
 *
 */
public class JsonParser extends UDF {

	public String evaluate(String jsonLine) {

		ObjectMapper objectMapper = new ObjectMapper();

		try {
			MovieRateBean bean = objectMapper.readValue(jsonLine, MovieRateBean.class);
			return bean.toString();
		} catch (Exception e) {

		}
		return "";
	}

}
