package hadoop_rpc.protocol;

public interface IUserLoginService {

	public static final long versionID = 1L;
	public String login(String name,String passwd);
	
}
